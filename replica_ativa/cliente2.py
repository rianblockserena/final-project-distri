from biblioteca import ClientRequest, GroupMember
from time import sleep
from random import randint
################################### TESTE ########################################

# Exemplo de uso
total_members = 3

#ID's de clientes comecam a partir do final dos ID's dos processos
total_clients = 2

total_processes = total_members + total_clients

#Dicionario com as portas (so localhost)
ports = {0:55550, 1:55551, 2:55552}


process_id = 3
client = ClientRequest(process_id, total_processes, ports)


sleep(5)

client.broadcast("Broadcast1 do C2")
sleep(randint(0,5))
client.broadcast("Broadcast2 do C2")
sleep(randint(0,5))
client.broadcast("Broadcast3 do C2")
sleep(randint(0,5))
client.broadcast("Broadcast4 do C2")
sleep(randint(0,5))
client.broadcast("Broadcast5 do C2")
sleep(randint(0,5))
client.broadcast("Broadcast6 do C2")
sleep(randint(0,5))
client.broadcast("Broadcast7 do C2")
sleep(randint(0,5))
client.broadcast("Broadcast8 do C2")
sleep(randint(0,5))
client.broadcast("Broadcast9 do C2")