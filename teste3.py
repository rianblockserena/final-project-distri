from collections import defaultdict
from time import sleep
import threading
import socket
import json
import uuid

class COCAAlgorithm:
    def __init__(self, process_id, total_processes, ports):
        self.process_id = process_id
        self.total_processes = total_processes
        #dicionario mapeando porta -> id
        self.ports = ports
        #Cada processo mantem uma lista com a quantia de entregas
        self.DELIV = [0] * total_processes
        #Cada processo mantem uma matriz com a quantia de envios (nao entregas) que ele no momento  
        self.SENT = [[0] * total_processes for _ in range(total_processes)]
        #Usado no multicast para definir sequenciador fixo
        self.sequencer = False
        #Mensagens pendentes do broadcast
        self.pending = set()


        #Usado para tratar os incrementos nas entregas
        self.lock = threading.Lock()

        #Usado para garantir ordem das entregas em broadcast
        self.broadcastlock = threading.Lock()

        #Criando socket server (para receber mensagens)
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = ("localhost", ports[process_id])
        self.server_socket.bind(server_address)
        self.server_socket.listen(self.total_processes)
        #Criando thread para receber as mensagens de outros processos
        self.thread_p = threading.Thread(target=self.receive_sock)
        self.thread_p.start()
    
    def init_sequencer(self):
        if self.sequencer:
            self.seqnum = 1
            threading.Thread(target=self.multicast_order).start()

    def broadcast(self, message):
        #Preparando dicionario multicast
        #gera um UUID, identificador unico universal
        message_id = uuid.uuid4()
        mess = (message, str(message_id))
        data = {'msg':mess, 'type':'multi', 'mat':None, 'id':-1}
        data_json = json.dumps(data).encode('utf-8')

        for dest_port in self.ports.values():
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            destination_address = ('localhost', dest_port)
            client_socket.connect(destination_address)
            
            #Envia com as garantias do tcp para cada um
            client_socket.sendall(data_json)
            client_socket.close()
        
    def receive_sock(self):
        while True:
            #Aceita a conexao tcp ("evitando" falhas do meio)
            client_socket, client_address = self.server_socket.accept()
            encoded_data = client_socket.recv(1024)
            data_json = encoded_data.decode('utf-8')
            data = json.loads(data_json)
            #Envia ao tratamento da mensagem
            self.receive_message(data['msg'], data['mat'], data['id'], data['type'])
            #Encerra conexao
            client_socket.close()



    def receive_message(self, message, stm, sender_id, type):
        if type == 'uni':
            #estamos criando uma thread para cada mensagem recebida ser avaliada ...
            threading.Thread(target=self.wait_for_delivery, args=(message, stm, sender_id,)).start()
        elif type == 'multi': #multi
            #tanto o sequenciador quanto os outros apenas adicionam, o sequenciador
            #tera uma thread cuidando de avisar a ordem aos demais
            self.pending.add(tuple(message))
        else: #"type == seq", numeros de sequencia enviados pelo sequencer
            #message eh o ID da mensagem a ser entregue escolhida pelo sequenciador
            #criamos uma thread pois a mensagem pode nao ter chego ainda
            threading.Thread(target=self.deliver_this, args=(message,)).start()

    def deliver_this(self, seq):
        #Lock para garantir ordem de chegada das mensagens
        self.broadcastlock.acquire()

        #Enquanto a mensagem nao estiver na lista espere
        while not any(x[1] == seq for x in self.pending):
            sleep(1)

        
        for m in self.pending:
            if m[1] == seq:
                self.pending.remove(m)
                message = m
                break
        
        self.deliver_message(message[0])

        self.broadcastlock.release()

    def multicast_order(self):
        while True:
            #Se existe ao menos uma mensagem pendente
            pend = list(self.pending)
            if len(pend)>0:
                #Retorna uma mensagem das pendentes, sets nao tem ordem
                #pend é uma dupla com (mensagem, id)
                self.send_order(pend[0][1])
                sleep(1)
            else:
                sleep(3)

    def send_order(self, seqnum):
        data = {'msg':seqnum, 'type':'seq', 'mat':None, 'id':-1}
        data_json = json.dumps(data).encode('utf-8')

        for dest_port in self.ports.values():
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            destination_address = ('localhost', dest_port)
            client_socket.connect(destination_address)
            
            #Envia com as garantias do tcp para cada um qual mensagem entregar
            client_socket.sendall(data_json)
            client_socket.close()

    def send_message(self, message, dest):
        
        #Criando ligacao tcp com outro processo
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        destination_address = ('localhost', self.ports[dest])
        client_socket.connect(destination_address)

    
        #Preparando dicionario para envio
        sent_copy = [list(row) for row in self.SENT]
        data = {'msg':message, 'mat':sent_copy, 'id':self.process_id, 'type':'uni'}
        data_json = json.dumps(data).encode('utf-8')

        #Envia com as garantias do tcp
        client_socket.sendall(data_json)
        client_socket.close()

        self.SENT[self.process_id][dest] += 1
        #Enviar pelo socket para destination
        
        #self.deliver_message(message_with_clock)
        
    def wait_for_delivery(self, message, stm, sender_id):
        #espera condicao para entregar -> algoritmo PDF: "The causal ordering abstraction and a simple way to implement it"
        while not all(self.DELIV[k] >= stm[k][self.process_id] for k in range(self.total_processes)):
            sleep(3)    #alterar para Condition depois, talvez
            print("acordei, vou testar")
            pass
        
        self.deliver_message(message)
        self.lock.acquire()
        self.DELIV[sender_id] += 1
        self.SENT[sender_id][self.process_id] += 1
        

        for k in range(self.total_processes):
            for l in range(self.total_processes):
                self.SENT[k][l] = max(self.SENT[k][l], stm[k][l])

        self.lock.release()

    def deliver_message(self, message):
        print(f"Process {self.process_id} received message: {message}")

################################### TESTE ########################################

# Exemplo de uso
total_processes = 3

#Dicionario com as portas (so localhost)
ports = {0:55550, 1:55551, 2:55552}


process_id = 2
process = COCAAlgorithm(process_id, total_processes, ports)

sleep(5)


process.send_message("Mensagem C1", 0)
process.send_message("Mensagem C2", 1)
process.send_message("Mensagem C3", 0)
process.send_message("Mensagem C4", 1)
process.send_message("Mensagem C5", 1)
