import socket, threading, time, json, sys
from enum import Enum

class MessageType(Enum):
    DISCOVERY_REQUEST = "discovery_request"
    DISCOVERY_RESPONSE = "discovery_response"
    ELECTION = "election"
    COORDINATOR = "coordinator"
    OK = "ok"
    HEARTBEAT = "heartbeat"
    HEARTBEAT_ACK = "heartbeat_ack"
    CHAT_MESSAGE = "chat_message"
    SERVER_ANNOUNCE = "server_announce"

class ServerState(Enum):
    FOLLOWER = "follower"
    LEADER = "leader"

class ChatServer:
    def __init__(self, server_id, port):
        self.server_id = server_id
        self.port = port
        self.ip = self._get_local_ip()
        self.state = ServerState.FOLLOWER
        self.leader_id = None
        self.servers = {}
        self.clients = {}
        self.running = False
        self.election_in_progress = False
        self.ok_received = False  # hinzugefügt
        self.last_heartbeat = {}

    def _get_local_ip(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        except:
            return "127.0.0.1"

    def start(self):
        self.running = True
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.udp_socket.bind(('', 12345))
        threading.Thread(target=self._handle_udp_messages, daemon=True).start()
        threading.Thread(target=self._send_server_announcements, daemon=True).start()
        threading.Thread(target=self._heartbeat_monitor, daemon=True).start()

        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.bind((self.ip, self.port))
        self.tcp_socket.listen()
        threading.Thread(target=self._handle_tcp_connections, daemon=True).start()

        time.sleep(2)
        self.start_election()
        print(f"Server {self.server_id} gestartet auf {self.ip}:{self.port}")

        while self.running:
            self._send_heartbeat()
            time.sleep(5)

    def _send_server_announcements(self):
        while self.running:
            msg = {
                "type": MessageType.SERVER_ANNOUNCE.value,
                "server_id": self.server_id,
                "ip": self.ip,
                "port": self.port
            }
            self.udp_socket.sendto(json.dumps(msg).encode(), ("255.255.255.255", 12345))
            time.sleep(5)

    def _send_heartbeat(self):
        print("[Logger] Ich sende jetzt Heartbeats an alle bekannten Server")
        msg = {
            "type": MessageType.HEARTBEAT.value,
            "sender_id": self.server_id,
            "leader_id": self.leader_id if self.state == ServerState.FOLLOWER else self.server_id,
            "timestamp": time.time()
        }
        for sid, (ip, port) in self.servers.items():
            if sid != self.server_id:
                self.udp_socket.sendto(json.dumps(msg).encode(), (ip, 12345))

    def _heartbeat_monitor(self):
        while self.running:
            print(f"[Monitor] Zustand: {self.state}, Leader: {self.leader_id}, Running: {self.running}")
            if self.state == ServerState.FOLLOWER and self.leader_id:
                print(f"[Monitor] Prüfung aktiviert. State: {self.state}, Leader ID: {self.leader_id}")
                last = self.last_heartbeat.get(self.leader_id, 0)
                delta = int(time.time() - last)
                print(f"[Monitor] Prüfe Leader {self.leader_id}, letztes Signal vor {delta}s")
                if delta > 6:
                    print("[Logger] Leader nicht erreichbar – starte neue Election")
                    self.start_election()

            if self.state == ServerState.LEADER:
                now = time.time()
                failed = []
                for sid, last in self.last_heartbeat.items():
                    if sid != self.server_id and now - last > 8:
                        print(f"[Monitor] Entferne Server {sid} (kein Heartbeat seit {int(now - last)}s)")
                        failed.append(sid)
                for sid in failed:
                    print(f"[Logger] Server {sid} entfernt – keine Verbindung mehr")
                    self.servers.pop(sid, None)
                    self.last_heartbeat.pop(sid, None)

            time.sleep(3)

    def _handle_udp_messages(self):
        while self.running:
            try:
                data, addr = self.udp_socket.recvfrom(1024)
                msg = json.loads(data.decode())
                self._route_udp_message(msg, addr)
            except: pass

    def _route_udp_message(self, msg, addr):
        t = msg.get("type")
        if t == MessageType.SERVER_ANNOUNCE.value:
            sid, ip, port = msg.get("server_id"), msg.get("ip"), msg.get("port")
            if sid != self.server_id and sid not in self.servers:
                self.servers[sid] = (ip, port)
                print(f"[Logger] Neuer Server entdeckt: ID={sid}, IP={ip}, PORT={port}")
                if sid > self.server_id:
                    print(f"[Logger] Entdeckter Server {sid} hat höhere ID als ich ({self.server_id}) – starte Election")
                    self.state = ServerState.FOLLOWER
                    self.leader_id = None
                    print("[Logger] Wechsel zu FOLLOWER – warte auf neuen Leader")
                    threading.Thread(target=self.start_election, daemon=True).start()
        elif t == MessageType.HEARTBEAT.value:
            leader_id = msg.get("leader_id")
            sender_id = msg.get("sender_id")
            print(f"[Logger] HEARTBEAT empfangen von Leader {leader_id}")

            if sender_id is not None and sender_id != self.server_id:
                self.last_heartbeat[sender_id] = time.time()
            if leader_id:
                self.last_heartbeat[leader_id] = time.time()
                if leader_id != self.server_id:
                    self.leader_id = leader_id
                    self.state = ServerState.FOLLOWER
                    print(f"[Logger] Setze Leader-ID auf {leader_id}, Wechsel zu FOLLOWER")
        elif t == MessageType.DISCOVERY_REQUEST.value and self.state == ServerState.LEADER:
            response = {
                "type": MessageType.DISCOVERY_RESPONSE.value,
                "leader_ip": self.ip,
                "leader_port": self.port
            }
            self.udp_socket.sendto(json.dumps(response).encode(), (addr[0], 12346))
        elif t == MessageType.ELECTION.value:
            print(f"[Logger] Election-Request von Server {msg.get('sender_id')}")
            if msg.get("sender_id") < self.server_id:
                print(f"[Logger] Sende OK an Server {msg.get('sender_id')}")
                ok = {"type": MessageType.OK.value, "sender_id": self.server_id}
                self.udp_socket.sendto(json.dumps(ok).encode(), addr)
                if not self.election_in_progress:
                    threading.Thread(target=self.start_election, daemon=True).start()
        elif t == MessageType.OK.value:
            print(f"[Logger] OK empfangen von Server {msg.get('sender_id')}")
            self.ok_received = True
            self.election_in_progress = False
        elif t == MessageType.COORDINATOR.value:
            new_leader = msg.get("leader_id")
            print(f"[Logger] COORDINATOR empfangen von Server {new_leader}")
            if new_leader != self.server_id:
                self.leader_id = new_leader
                self.state = ServerState.FOLLOWER
                print(f"[Logger] Wechsel zu FOLLOWER – Leader ist jetzt {new_leader}")
            else:
                print(f"[Logger] Ignoriere COORDINATOR von mir selbst ({new_leader})")
            self.election_in_progress = False

    def start_election(self):
        print(f"[Logger] Starte Election von Server {self.server_id}")
        self.election_in_progress = True
        self.ok_received = False

        higher = [sid for sid in self.servers if sid > self.server_id]
        if not higher:
            self.become_leader()
            return

        msg = {"type": MessageType.ELECTION.value, "sender_id": self.server_id}
        for sid in higher:
            ip, port = self.servers[sid]
            self.udp_socket.sendto(json.dumps(msg).encode(), (ip, 12345))

        time.sleep(3)
        if self.election_in_progress and not self.ok_received:
            self.become_leader()

    def become_leader(self):
        self.state = ServerState.LEADER
        self.leader_id = self.server_id
        self.election_in_progress = False
        print(f"[Logger] Server {self.server_id} ist jetzt Leader")
        msg = {"type": MessageType.COORDINATOR.value, "leader_id": self.server_id}
        for sid, (ip, port) in self.servers.items():
            if sid != self.server_id:
                self.udp_socket.sendto(json.dumps(msg).encode(), (ip, 12345))

    def _handle_tcp_connections(self):
        while self.running:
            client_socket, addr = self.tcp_socket.accept()
            self.clients[client_socket] = addr
            threading.Thread(target=self._handle_client, args=(client_socket,), daemon=True).start()

    def _handle_client(self, sock):
        while self.running:
            try:
                data = sock.recv(1024).decode()
                if not data:
                    break
                print(f"[Server] Nachricht empfangen: {data}")
                for client in self.clients:
                    if client != sock:
                        client.send(data.encode())
            except: break
        sock.close()
        self.clients.pop(sock, None)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python server.py <server_id> <port>")
        sys.exit(1)

    server_id = int(sys.argv[1])
    port = int(sys.argv[2])
    server = ChatServer(server_id, port)
    server.start()
