from time import sleep
import threading
import socket
import json
import uuid

class ClientRequest():
    def __init__(self, process_id, total_processes, ports):
        self.total_processes = total_processes
        self.ports = ports
        self.process_id = process_id
        self.SENT = [[0] * self.total_processes for _ in range(self.total_processes)]

    def send(self, message, dest):
        try:
            destination_address = ('localhost', self.ports[dest])
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect(destination_address)

            #Preparando dicionario para envio
            sent_copy = [list(row) for row in self.SENT]
            data = {'msg':message, 'mat':sent_copy, 'id':self.process_id, 'type':'uni'}
            data_json = json.dumps(data).encode('utf-8')

            #Envia com as garantias do tcp
            client_socket.sendall(data_json)
            client_socket.close()

            self.SENT[self.process_id][dest] += 1
        except:
            print("Erro no send de ClientRequest")

    def broadcast(self, message):
        #Preparando dicionario multicast
        #gera um UUID, identificador unico universal
        message_id = uuid.uuid4()
        mess = (message, str(message_id))
        data = {'msg':mess, 'type':'multi', 'mat':None, 'id':-1}
        data_json = json.dumps(data).encode('utf-8')

        #
        for dest_port in self.ports.values():
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            destination_address = ('localhost', dest_port)
            client_socket.connect(destination_address)
            
            #Envia com as garantias do tcp para cada um
            client_socket.sendall(data_json)
            client_socket.close()


class GroupMember:
    def __init__(self, process_id, total_processes, ports):
        self.process_id = process_id
        self.total_processes = total_processes
        #dicionario mapeando porta -> id
        self.ports = ports
        #Cada processo mantem uma lista com a quantia de entregas
        self.DELIV = [0] * self.total_processes
        #Cada processo mantem uma matriz com a quantia de envios (nao entregas) que ele no momento  
        self.SENT = [[0] * self.total_processes for _ in range(self.total_processes)]
        #Usado no multicast para definir sequenciador fixo
        self.sequencer = False
        #Mensagens pendentes do broadcast
        self.pending = []


        #Usado para tratar os incrementos nas entregas
        self.lock = threading.Lock()

        #Usado para garantir ordem das entregas em broadcast
        self.broadcastlock = threading.Lock()

        self.filaBroad = threading.Lock()
        self.filaUni = threading.Lock()
        self.deliverU = []
        self.deliverB = []
        self.condUni = threading.Condition()
        self.condBroad = threading.Condition()
        

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
            self.pending.append(tuple(message))
        else: #"type == seq", numeros de sequencia enviados pelo sequencer
            #message eh o numero de sequencia enviado pelo sequenciador
            threading.Thread(target=self.wait_for_delivery, args=(message, stm, sender_id, True,)).start()

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
        
        self.deliver_broadcast(message[0])
        self.broadcastlock.release()

    def multicast_order(self):
        last_sent = 0
        while True:
            #Se existe ao menos uma mensagem pendente
            #pend = self.pending
            try:
                if len(self.pending)>0 and self.pending[0][1] != last_sent:
                    #Retorna uma mensagem das pendentes, sets nao tem ordem
                    #pend é uma dupla com (mensagem, id)
                    last_sent = self.pending[0][1]
                    self.send_order(self.pending[0][1])
                    sleep(1)
                else:
                    sleep(3)
            except:
                sleep(3)

    def send_order(self, seqnum):
        #Manda para todos qual mensagem aceitar, respeitando a ordem
        for dest_id in self.ports.keys():
           self.send_message(seqnum, dest_id, type='seq')


    def send_message(self, message, dest, type='uni'):
        
        #Criando ligacao tcp com outro processo
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        destination_address = ('localhost', self.ports[dest])
        client_socket.connect(destination_address)

    
        #Preparando dicionario para envio
        sent_copy = [list(row) for row in self.SENT]
        data = {'msg':message, 'mat':sent_copy, 'id':self.process_id, 'type':type}
        data_json = json.dumps(data).encode('utf-8')

        #Envia com as garantias do tcp
        client_socket.sendall(data_json)
        client_socket.close()

        #para nao dar problema no broadcast, pois precisa fazer um send para si mesmo quando for o sequencer
        if type == 'uni':
            self.SENT[self.process_id][dest] += 1
        #Enviar pelo socket para destination
        
        #self.deliver_message(message_with_clock)
        
    def wait_for_delivery(self, message, stm, sender_id, seq=False):
        #espera condicao para entregar -> algoritmo PDF: "The causal ordering abstraction and a simple way to implement it"
        while not all(self.DELIV[k] >= stm[k][self.process_id] for k in range(self.total_processes)):
            sleep(3)    #alterar para Condition depois, talvez


        if not seq:
            self.deliver_unicast(message)
        else:
            self.deliver_this(message)
            
        self.lock.acquire()
        self.DELIV[sender_id] += 1

        self.SENT[sender_id][self.process_id] += 1
        

        for k in range(self.total_processes):
            for l in range(self.total_processes):
                self.SENT[k][l] = max(self.SENT[k][l], stm[k][l])

        self.lock.release()

    def deliver_unicast(self, message):
        self.filaUni.acquire()
        print(f"Process {self.process_id} received unicast message: {message}")
        self.deliverU.append(message)
        with self.condUni:
            self.condUni.notify()
        self.filaUni.release()

    def deliver_broadcast(self, message):
        self.filaBroad.acquire()
        #print(f"Process {self.process_id} received broadcast message: {message}")
        self.deliverB.append(message)
        with self.condBroad:
            self.condBroad.notify()
        self.filaBroad.release()

    def receive(self): #UNICAST
        with self.condUni:
            while len(self.deliverU) == 0:  # Aguarda até que a fila unicast não esteja vazia
                self.condUni.wait()

        self.filaUni.acquire()
        message = self.deliverU.pop(0)
        self.filaUni.release()
        return message
  

    def deliver(self): #BROADCAST
        with self.condBroad:
            while len(self.deliverB) == 0:  # Aguarda até que a fila unicast não esteja vazia
                self.condBroad.wait()

            #Locks para garantir condicao de corrida com os metodos que poem na fila
        self.filaBroad.acquire()
        message = self.deliverB.pop(0)
        self.filaBroad.release()
        return message



################################### TESTE ########################################
'''
# Exemplo de uso
total_members = 3

#ID's de clientes comecam a partir do final dos ID's dos processos
total_clients = 2

total_processes = total_members + total_clients

#Dicionario com as portas (so localhost)
ports = {0:55550, 1:55551, 2:55552}


process_id = 0
process = GroupMember(process_id, total_processes, ports)
process.sequencer = True
process.init_sequencer()

sleep(5)

#process.broadcast("MENSAGEM EM BROADCAST POR p0")

#process.send_message("Mensagem A1", 1)
#process.send_message("Mensagem A2", 2)
#process.send_message("Mensagem A3", 1)
#process.send_message("Mensagem A4", 1)
#process.send_message("Mensagem A5", 2)
'''
