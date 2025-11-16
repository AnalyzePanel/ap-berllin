import json
import socket
import threading
import time


def read(conn, addr):
    try:
        # Set socket timeout to detect disconnections (30 seconds)
        conn.settimeout(30.0)
        
        # Enable TCP keepalive
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        
        # On Linux, configure keepalive parameters
        # On Windows, these might not be available, so wrap in try-except
        try:
            # TCP_KEEPIDLE: time before sending first keepalive probe (in seconds)
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
            # TCP_KEEPINTVL: interval between keepalive probes (in seconds)
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
            # TCP_KEEPCNT: number of keepalive probes before considering connection dead
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
        except (AttributeError, OSError):
            # Windows doesn't support these options, that's OK
            pass
        
        buffer = ""
        while True:
            try:
                data = conn.recv(1024).decode('utf-8', errors='ignore')
                if not data:
                    # Connection closed by client
                    break
                
                buffer += data
                
                # Process complete messages (messages end with \n)
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    if line.strip():
                        try:
                            msg_json = json.loads(line)
                            msg_type = msg_json.get('type', 'unknown')
                            
                            # Handle heartbeat messages silently
                            if msg_type == 'heartbeat':
                                print(f"[INFO] Heartbeat from {addr} (login: {msg_json.get('login', 'unknown')})")
                            else:
                                print(f"[INFO] Message from {addr} (type: {msg_type}): {line[:200]}...")
                        except json.JSONDecodeError:
                            print(f"[WARN] Invalid JSON from {addr}: {line[:100]}")
                            
            except socket.timeout:
                # Timeout is OK, just continue (connection might still be alive)
                # Send a keepalive or just continue
                continue
            except ConnectionResetError:
                # Connection was reset by client
                print(f"[INFO] Connection reset by {addr}")
                break
            except BrokenPipeError:
                # Connection broken
                print(f"[INFO] Broken pipe with {addr}")
                break
                
    except Exception as e:
        print(f"[ERROR] read from {addr} -> {e}")
    finally:
        try:
            conn.close()
        except:
            pass
        print(f"[INFO] Connection closed with {addr}")


def write(conn, addr):
    try:
        # Set socket timeout
        conn.settimeout(30.0)
        
        while True:
            try:
                payload = { "cmd": "ACCOUNT" }
                conn.send(json.dumps(payload).encode())
                time.sleep(3)
            except (ConnectionResetError, BrokenPipeError, OSError) as e:
                # Connection lost, exit write thread
                print(f"[INFO] Write thread ending for {addr}: {e}")
                break
            except socket.timeout:
                # Timeout, but connection might still be alive, continue
                continue
    except Exception as e:
        print(f"[ERROR] write to {addr} -> {e}")
    finally:
        try:
            conn.close()
        except:
            pass


def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Enable socket reuse to avoid "Address already in use" errors
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    # Enable TCP keepalive on server socket
    server.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    
    server.bind(("0.0.0.0", 8888))  # accessible from Docker
    server.listen(10)
    print("[INFO] Server is listening on port 8888", server.getsockname())
    print("[INFO] TCP keepalive enabled, timeout: 30s")

    while True:
        try:
            conn, addr = server.accept()
            print(f"[INFO] Connection established with {addr}")
            
            # Set socket options on accepted connection
            conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            
            # Set send/receive buffer sizes for better performance
            conn.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65536)
            conn.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)

            thread_r = threading.Thread(target=read, args=(conn, addr))
            thread_w = threading.Thread(target=write, args=(conn, addr))

            thread_r.daemon = True
            thread_w.daemon = True

            thread_r.start()
            thread_w.start()
        except Exception as e:
            print(f"[ERROR] Error accepting connection: {e}")
            continue


if __name__ == "__main__":
    start_server()
