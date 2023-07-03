from biblioteca import ClientRequest, GroupMember
from time import sleep

################################### TESTE ########################################

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

lista_msg = []


while True:
    msg = process.receive()
    #recebeu da replicacao
    if msg.startswith("@"):
        mensagem = msg[1:]
        lista_msg.append(mensagem)
    else: #recebeu do cliente
        lista_msg.append(msg)
        mensagem = "@"+msg
        for i in range(total_members):
            if i != process_id:
                process.send_message(mensagem, i)


#process.broadcast("MENSAGEM EM BROADCAST POR p0")

#process.send_message("Mensagem A1", 1)
#process.send_message("Mensagem A2", 2)
#process.send_message("Mensagem A3", 1)
#process.send_message("Mensagem A4", 1)
#process.send_message("Mensagem A5", 2)
