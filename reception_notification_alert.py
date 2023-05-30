import os
from dotenv import load_dotenv
from google.cloud import pubsub_v1
load_dotenv()
# For google storage
credential_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
# Définir le nom du sujet et le message à envoyer
topic_name = os.getenv("TOPIC_NAME")
souscription = os.getenv("SOUSCRIPTION")

# Instancier un client d'abonnement
subscriber = pubsub_v1.SubscriberClient()

# Définir la fonction de rappel pour traiter les messages reçus


def callback(message):
    print("\n", message.data)
    message.ack()


# Lier la fonction de rappel à l'abonnement et démarrer la réception des messages
print("En attente de message ....")
subscriber.subscribe(souscription, callback=callback)

# Bloquer le processus principal pour continuer à recevoir les messages
while True:
    pass
