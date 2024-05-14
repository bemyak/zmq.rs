use crate::backend::Peer;
use crate::codec::{FramedIo, Message, ZmqFramedRead};
use crate::endpoint::Endpoint;
use crate::error::ZmqResult;
use crate::fair_queue::FairQueue;
use crate::fair_queue::QueueInner;
use crate::message::ZmqMessage;
use crate::transport::AcceptStopHandle;
use crate::util::{self, PeerIdentity};
use crate::{
    async_rt, MultiPeerBackend, Socket, SocketBackend, SocketEvent, SocketOptions, SocketRecv,
    SocketType,
};

use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use crossbeam_queue::SegQueue;
use dashmap::DashMap;
use futures_channel::mpsc;
use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub enum SubBackendMsgType {
    UNSUBSCRIBE = 0,
    SUBSCRIBE = 1,
}

pub(crate) struct SubSocketBackend {
    pub(crate) peers: DashMap<PeerIdentity, Peer>,
    fair_queue_inner: Option<Arc<Mutex<QueueInner<ZmqFramedRead, PeerIdentity>>>>,
    pub(crate) round_robin: SegQueue<PeerIdentity>,
    socket_type: SocketType,
    socket_options: SocketOptions,
    pub(crate) socket_monitor: Mutex<Option<mpsc::Sender<SocketEvent>>>,
    subs: Mutex<HashSet<String>>,
    connect_endpoints: DashMap<PeerIdentity, Endpoint>,
}

impl SubSocketBackend {
    pub(crate) fn with_options(
        fair_queue_inner: Option<Arc<Mutex<QueueInner<ZmqFramedRead, PeerIdentity>>>>,
        socket_type: SocketType,
        options: SocketOptions,
    ) -> Self {
        Self {
            peers: DashMap::new(),
            fair_queue_inner,
            round_robin: SegQueue::new(),
            socket_type,
            socket_options: options,
            socket_monitor: Mutex::new(None),
            subs: Mutex::new(HashSet::new()),
            connect_endpoints: DashMap::new(),
        }
    }

    pub fn create_subs_message(subscription: &str, msg_type: SubBackendMsgType) -> ZmqMessage {
        let mut buf = BytesMut::with_capacity(subscription.len() + 1);
        buf.put_u8(msg_type as u8);
        buf.extend_from_slice(subscription.as_bytes());

        buf.freeze().into()
    }
}

impl SocketBackend for SubSocketBackend {
    fn socket_type(&self) -> SocketType {
        self.socket_type
    }

    fn socket_options(&self) -> &SocketOptions {
        &self.socket_options
    }

    fn shutdown(&self) {
        self.peers.clear();
    }

    fn monitor(&self) -> &Mutex<Option<mpsc::Sender<SocketEvent>>> {
        &self.socket_monitor
    }
}

#[async_trait]
impl MultiPeerBackend for SubSocketBackend {
    async fn peer_connected(
        self: Arc<Self>,
        peer_id: &PeerIdentity,
        io: FramedIo,
        endpoint: Option<Endpoint>,
    ) {
        let (recv_queue, mut send_queue) = io.into_parts();

        let subs_msgs: Vec<ZmqMessage> = self
            .subs
            .lock()
            .iter()
            .map(|x| SubSocketBackend::create_subs_message(x, SubBackendMsgType::SUBSCRIBE))
            .collect();

        for message in subs_msgs {
            send_queue.send(Message::Message(message)).await.unwrap();
        }

        self.peers.insert(peer_id.clone(), Peer { send_queue });
        self.round_robin.push(peer_id.clone());

        if let Some(queue_inner) = &self.fair_queue_inner {
            queue_inner.lock().insert(peer_id.clone(), recv_queue);
        }

        if let Some(e) = endpoint {
            self.connect_endpoints.insert(peer_id.clone(), e);
        }
    }

    fn peer_disconnected(self: Arc<Self>, peer_id: &PeerIdentity) {
        if let Some(monitor) = self.monitor().lock().as_mut() {
            let _ = monitor.try_send(SocketEvent::Disconnected(peer_id.clone()));
        }

        self.peers.remove(peer_id);

        if let Some(inner) = &self.fair_queue_inner {
            inner.lock().remove(peer_id);
        }

        let endpoint = match self.connect_endpoints.remove(peer_id) {
            Some((_, e)) => e,
            None => return,
        };
        let backend = self;

        async_rt::task::spawn(async move {
            let (socket, endpoint) = util::connect_forever(endpoint)
                .await
                .expect("Failed to connect");
            let peer_id = util::peer_connected(socket, backend.clone(), Some(endpoint.clone()))
                .await
                .expect("Failed to handshake");
            if let Some(monitor) = backend.monitor().lock().as_mut() {
                let _ = monitor.try_send(SocketEvent::Connected(endpoint, peer_id));
            }
        });
    }
}

pub struct SubSocket {
    backend: Arc<SubSocketBackend>,
    fair_queue: FairQueue<ZmqFramedRead, PeerIdentity>,
    binds: HashMap<Endpoint, AcceptStopHandle>,
}

impl Drop for SubSocket {
    fn drop(&mut self) {
        self.backend.shutdown()
    }
}

impl SubSocket {
    pub async fn subscribe(&mut self, subscription: &str) -> ZmqResult<()> {
        self.backend.subs.lock().insert(subscription.to_string());
        self.process_subs(subscription, SubBackendMsgType::SUBSCRIBE)
            .await
    }

    pub async fn unsubscribe(&mut self, subscription: &str) -> ZmqResult<()> {
        self.backend.subs.lock().remove(subscription);
        self.process_subs(subscription, SubBackendMsgType::UNSUBSCRIBE)
            .await
    }

    async fn process_subs(
        &mut self,
        subscription: &str,
        msg_type: SubBackendMsgType,
    ) -> ZmqResult<()> {
        let message: ZmqMessage = SubSocketBackend::create_subs_message(subscription, msg_type);

        for mut peer in self.backend.peers.iter_mut() {
            peer.send_queue
                .send(Message::Message(message.clone()))
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Socket for SubSocket {
    fn with_options(options: SocketOptions) -> Self {
        let fair_queue = FairQueue::new(options.block_on_no_clients);
        Self {
            backend: Arc::new(SubSocketBackend::with_options(
                Some(fair_queue.inner()),
                SocketType::SUB,
                options,
            )),
            fair_queue,
            binds: HashMap::new(),
        }
    }

    fn backend(&self) -> Arc<dyn MultiPeerBackend> {
        self.backend.clone()
    }

    fn binds(&mut self) -> &mut HashMap<Endpoint, AcceptStopHandle> {
        &mut self.binds
    }

    fn monitor(&mut self) -> mpsc::Receiver<SocketEvent> {
        let (sender, receiver) = mpsc::channel(1024);
        self.backend.socket_monitor.lock().replace(sender);
        receiver
    }
}

#[async_trait]
impl SocketRecv for SubSocket {
    async fn recv(&mut self) -> ZmqResult<ZmqMessage> {
        loop {
            match self.fair_queue.next().await {
                Some((_peer_id, Ok(Message::Message(message)))) => {
                    return Ok(message);
                }
                Some((_peer_id, Ok(msg))) => todo!("Unimplemented message: {:?}", msg),
                Some((peer_id, Err(_))) => {
                    self.backend.clone().peer_disconnected(&peer_id);
                }
                None => {
                    // All clients disconnected
                    let backend = self.backend.clone();
                    let mut peer_ids = Vec::with_capacity(backend.peers.len());
                    for peer in &backend.peers {
                        let peer_id = peer.key().clone();
                        peer_ids.push(peer_id);
                    }
                    for peer_id in peer_ids {
                        backend.clone().peer_disconnected(&peer_id);
                    }
                }
            }
        }
    }
}
