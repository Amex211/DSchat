import socket, threading, json, time, sys

DISCOVERY_PORT = 12345
BROADCAST_ADDR = '255.255.255.255'
RESPONSE_PORT = 12346

class MessageType:
    DISCOVERY_REQUEST = "discovery_request"
    DISCOVERY_RESPONSE = "discovery_response"
    CHAT_MESSAGE = "chat_message"

def discover_leader():
    print("[Client] Discovery request is being sent...")
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    msg = {"type": MessageType.DISCOVERY_REQUEST}
    try:
        sock.sendto(json.dumps(msg).encode(), (BROADCAST_ADDR, DISCOVERY_PORT))
    except Exception as e:
        print(f"[Client] Error sending discovery request: {e}")
    
    recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    recv_sock.bind(("", RESPONSE_PORT))
    recv_sock.settimeout(5)
    try:
        data, addr = recv_sock.recvfrom(1024)
        response = json.loads(data.decode())
        if response.get("type") == MessageType.DISCOVERY_RESPONSE:
            print(f"[Client] Response received from server: {response}")
            return (response["leader_ip"], response["leader_port"])
    except socket.timeout:
        print("[Client] Timeout – no response from server.")
    except Exception as e:
        print(f"[Client] Error receiving response: {e}")
    finally:
        recv_sock.close()
    return None, None

def receive_messages(sock, username):
    while True:
        try:
            data = sock.recv(1024)
            if not data:
                raise ConnectionError("Connection was closed by server.")
            print("\n[Chat]", data.decode())
        except Exception as e:
            print(f"\n[Client] Connection lost: {e}")
            sock.close()
            reconnect(username)
            break

def reconnect(username):
    print("[Client] Trying to find new leader...")
    time.sleep(2)
    new_ip, new_port = discover_leader()
    if new_ip:
        start_chat(username, new_ip, new_port)
    else:
        print("[Client] No new leader found. Exiting client.")
        sys.exit(1)

def start_chat(username, ip, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((ip, port))
        print(f"[Client] Connected to server: {ip}:{port}")
        sock.send(f"[SYSTEM] {username} has joined the chat.".encode())
    except Exception as e:
        print(f"[Client] Connection failed: {e}")
        reconnect(username)
        return
    
    threading.Thread(target=receive_messages, args=(sock, username), daemon=True).start()
    
    try:
        while True:
            msg = input()
            if msg.lower() == "exit":
                break
            full_msg = f"{username}: {msg}"
            try:
                sock.send(full_msg.encode())
            except Exception as e:
                print(f"[Client] Error sending message: {e}")
                sock.close()
                reconnect(username)
                break
    except KeyboardInterrupt:
        pass
    finally:
        sock.close()

def start_client():
    if len(sys.argv) < 2:
        print("Usage: python client.py <username>")
        sys.exit(1)
    
    username = sys.argv[1]
    ip, port = discover_leader()
    if not ip:
        print("[Client] No server found.")
        return
    
    start_chat(username, ip, port)

if __name__ == '__main__':
    start_client()