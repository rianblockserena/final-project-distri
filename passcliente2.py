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


process_id = 4
client = ClientRequest(process_id, total_processes, ports)

sleep(5)

client.send("Mensagem1 do C2", 0)
client.send("Mensagem1 do C2", 1)
client.send("Mensagem1 do C2", 2)
#client.broadcast("broadcast1 do C2---")
sleep(randint(0,5))
client.send("Mensagem2 do C2", 0)
client.send("Mensagem2 do C2", 1)
client.send("Mensagem3 do C2", 1)
client.send("Mensagem2 do C2", 2)
#client.broadcast("broadcast2 do C2---")
sleep(randint(0,5))
#client.broadcast("broadcast3 do C2---")
sleep(randint(0,5))
client.send("Mensagem4 do C2", 1)
client.send("Mensagem3 do C2", 2)
client.send("Mensagem3 do C2", 0)
client.send("Mensagem5 do C2", 1)
client.send("Mensagem6 do C2", 1)
client.send("Mensagem4 do C2", 2)
#client.broadcast("broadcast4 do C2---")
sleep(randint(0,5))
#client.broadcast("broadcast5 do C2---")
sleep(randint(0,5))
#client.broadcast("broadcast6 do C2---")
sleep(randint(0,5))
#client.broadcast("broadcast7 do C2---")
sleep(randint(0,5))
#client.broadcast("broadcast8 do C2---")
