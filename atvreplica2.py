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


process_id = 1
process = GroupMember(process_id, total_processes, ports)

sleep(5)

while True:
    msg = process.deliver()
    print(msg)
