from pymongo import MongoClient

host= "rhea.isegi.unl.pt"
port= "28014"
user= "GROUP_14"
password= "MjI2ODk4OTAxMTcwNDU4NTEwOTk3MTc2NzA5Nzg2NzAzNDg2MDM4"
protocol = "mongodb"

client = MongoClient(f"{protocol}://{user}:{password}@{host}:{port}")
db = client.contracts
eu = db.eu