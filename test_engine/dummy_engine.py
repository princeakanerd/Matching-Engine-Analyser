import socket
import threading

def handle_client(conn, addr):
    print(f"[Contestant Engine] New connection from Bot Fleet {addr}", flush=True)
    order_count = 0
    try:
        while True:
            data = conn.recv(8192)
            if not data:
                break
            # Count the number of newline characters (each line is an order)
            order_count += data.count(b'\n')
    except Exception as e:
        pass
    
    print(f"[Contestant Engine] Bot disconnected. Processed {order_count} total orders.", flush=True)

def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Bind to 0.0.0.0 so the Docker host can access it
    server.bind(('0.0.0.0', 9000))
    server.listen(100)
    print("[Contestant Engine] High-Frequency Matching Engine Booted Up!", flush=True)
    print("[Contestant Engine] Listening for incoming orders on Port 9000...", flush=True)

    while True:
        conn, addr = server.accept()
        # Spawn a new thread to handle this specific Bot concurrently
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()

if __name__ == "__main__":
    start_server()
