# SSH to VM
ssh -i C:\Users\Thanh Phat\Downloads\azure_key_pair\master-vm_key.pem azureuser@4.194.249.204 # Master VM
ssh -i C:\Users\Thanh Phat\Downloads\azure_key_pair\master-vm_key.pem azureuser@172.188.16.170 # Worker VM-1
ssh -i C:\Users\Thanh Phat\Downloads\azure_key_pair\master-vm_key.pem azureuser@172.188.83.134 # Worker VM-2


# Migrate Folder
scp -r -i ./dagster-key-pair.pem N:/DE_Project/dagster-demo azureuser@20.2.141.56:/home/azureuser/dagster-demo
scp -r -i ./dagster-key-pair.pem N:/DE_Project/dagster-demo azureuser@52.237.82.255:/home/azureuser/dagster-demo
scp -r -i ./dagster-key-pair.pem N:/DE_Project/dagster-demo azureuser@20.205.19.175:/home/azureuser/dagster-demo

# Set up Network
## Come to Master Network Security Group and Config Inbound Rules:
- Add Rule: Allow TCP 5432 from Worker Subnet (for PostgreSQL)
- Add Rule: Allow TCP 6379 from Worker Subnet (for Redis)
- Add Rule: Allow TCP 3000 from Master (for Dagster UI)


# Create dagster.yaml inside dagster_home on each worker VM same config with master but change Master IP
## Note: For VM worker dagster.yaml just only in dagster_home, not in worker folder