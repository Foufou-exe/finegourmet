import os
import zipfile
import shutil

def extract_and_organize_data(zip_path, extract_to):
    # Vérifiez si le fichier zip existe
    if not os.path.exists(zip_path):
        print(f"Le fichier {zip_path} n'existe pas.")
        return

    # Créez le répertoire de destination s'il n'existe pas
    if not os.path.exists(extract_to):
        os.makedirs(extract_to)

    # Ouvrez et extrayez le fichier zip
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

    # Parcourez les fichiers extraits et organisez-les
    for root, _, files in os.walk(extract_to):
        for file in files:
            if "sfcc_sales" in file:
                destination_folder = os.path.join(extract_to, 'salesforce')
            elif "boutiques" in file:
                destination_folder = os.path.join(extract_to, 'boutiques')
            elif "cegid" in file:
                destination_folder = os.path.join(extract_to, 'cegid')
            elif "product_reference" in file:
                destination_folder = os.path.join(extract_to, 'product')
            else:
                # Si aucune condition n'est remplie, le fichier reste dans le dossier actuel
                continue

            # Créez le dossier de destination s'il n'existe pas
            if not os.path.exists(destination_folder):
                os.makedirs(destination_folder)

            # Déplacez le fichier vers le dossier de destination
            source_path = os.path.join(root, file)
            destination_path = os.path.join(destination_folder, file)
            shutil.move(source_path, destination_path)

    print(f"Extraction et organisation terminées. Les fichiers ont été organisés dans {extract_to}.")

if __name__ == "__main__":
    # Chemin vers le fichier zip à la racine du projet
    zip_path = 'Data.zip'

    # Répertoire où extraire et organiser les fichiers
    extract_to = 'data'

    # Appelez la fonction pour extraire et organiser les données
    extract_and_organize_data(zip_path, extract_to)
