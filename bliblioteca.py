import socket

# Variáveis globais
receiveds = set()
token = {
    "seqnum": 0,
    "sequenced": set()
}

def TO_broadcast(m):
    sequenciadores = ["192.168.0.1", "192.168.0.2", "192.168.0.3"]  # Exemplo de endereços IP dos sequenciadores

    for sequenciador_ip in sequenciadores:
        try:
            # Cria um socket UDP
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            
            # Envia a mensagem m para o sequenciador usando o endereço IP
            sock.sendto(m.encode(), (sequenciador_ip, 1234))  # Substitua 1234 pela porta adequada do sequenciador
            
            # Fecha o socket após enviar a mensagem
            sock.close()
            
        except socket.error as e:
            print(f"Erro ao enviar a mensagem para o sequenciador {sequenciador_ip}: {e}")

def initialize_process(si, n):
    global receiveds, token
    receiveds = set()

    if si == 1:
        token["seqnum"] = 1
        token["sequenced"] = set()
        send_token(token, 1, n)

def process_receive_message(si, m, destinations):
    global receiveds, token
    receiveds = receiveds.union({m})

    for m_prime in (receiveds - token["sequenced"]):
        send_message(m_prime, token["seqnum"], destinations)
        token["seqnum"] += 1
        token["sequenced"].add(m_prime)

    send_token(token, si, n)

def send_message(message, seqnum, destinations):
    for destination in destinations:
        try:
            # Cria um socket UDP
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            
            # Envia a mensagem e o número de sequência para o destino usando o endereço IP
            sock.sendto(f"{message},{seqnum}".encode(), (destination, 5678))  # Substitua 5678 pela porta adequada do destino
            
            # Fecha o socket após enviar a mensagem
            sock.close()
            
        except socket.error as e:
            print(f"Erro ao enviar a mensagem para o destino {destination}: {e}")

def send_token(token, si, n):
    try:
        # Cria um socket UDP
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        # Serializa o token para envio
        token_data = f"{token['seqnum']},{','.join(token['sequenced'])}".encode()
        
        # Envia o token para o próximo processo Si+1 (mod n) usando o endereço IP
        destination_ip = sequenciadores[(si + 1) % n - 1]  # Obtém o próximo sequenciador da lista
        sock.sendto(token_data, (destination_ip, 1234))  # Substitua 1234 pela porta adequada do próximo sequenciador
        
        # Fecha o socket após enviar o token
        sock.close()
        
    except socket.error as e:
        print(f"Erro ao enviar o token para o próximo sequenciador: {e}")

def initialize_destinations(pi):
    nextdeliverpi = 1
    pendingpi = []

def process_receive_token(pi, token):
    pendingpi.append(token)

    while any(seqnum == nextdeliverpi for _, seqnum in pendingpi):



#---------------------------------------------------CAUSAL-------------------------------------------

from collections import defaultdict

class COCAAlgorithm:
    def __init__(self, process_id, total_processes):
        self.process_id = process_id
        self.total_processes = total_processes
        self.clock = defaultdict(int)
        self.pending_messages = []

    def send_message(self, message):
        self.clock[self.process_id] += 1
        message_with_clock = (message, dict(self.clock))
        self.deliver_message(message_with_clock)

    def receive_message(self, sender_id, message_with_clock):
        message, sender_clock = message_with_clock
        self.clock[sender_id] = max(self.clock[sender_id], sender_clock[sender_id]) + 1

        self.pending_messages.append((sender_id, message_with_clock))
        self.process_pending_messages()

    def process_pending_messages(self):
        pending_messages_copy = self.pending_messages[:]
        for sender_id, message_with_clock in pending_messages_copy:
            message, sender_clock = message_with_clock

            causal_order_respected = all(
                self.clock[process_id] >= sender_clock[process_id] for process_id in range(self.total_processes)
                if process_id != sender_id
            )

            if causal_order_respected:
                self.pending_messages.remove((sender_id, message_with_clock))
                self.deliver_message(message_with_clock)

    def deliver_message(self, message_with_clock):
        message, clock = message_with_clock
        print(f"Process {self.process_id} received message: {message} with clock: {clock}")


# Exemplo de uso
processes = []
total_processes = 3

# Inicialização dos processos
for process_id in range(total_processes):
    process = COCAAlgorithm(process_id, total_processes)
    processes.append(process)

# Simulação de troca de mensagens
processes[0].send_message("Mensagem 1")
processes[2].send_message("Mensagem 2")
processes[1].send_message("Mensagem 3")
processes[1].send_message("Mensagem 4")
processes[0].send_message("Mensagem 5")
