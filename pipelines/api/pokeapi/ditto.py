import dlt
import requests

def get_ditto():
    """Récupère le Pokémon 'ditto' depuis l'API PokéAPI"""
    resp = requests.get("https://pokeapi.co/api/v2/pokemon/ditto")
    resp.raise_for_status()
    # dlt attend un iterable
    return [resp.json()]

# Crée le pipeline dlt
pipeline = dlt.pipeline(
    pipeline_name="pokeapi_ditto",
    destination="filesystem",
    dataset_name="pokemon_data"
)

# Exécute et charge la donnée
load_info = pipeline.run(get_ditto(), table_name="ditto")

print("Pipeline terminé. Infos du chargement :", load_info)