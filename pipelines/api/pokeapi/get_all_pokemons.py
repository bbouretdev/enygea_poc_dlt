import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Client REST pour la PokeAPI
pokeapi_client = RESTClient(
    base_url="https://pokeapi.co/api/v2/",
    headers={"Content-Type": "application/json"},
    paginator=JSONLinkPaginator(next_url_path="next"),
    data_selector="results"
)

# --------------------------------------------------
# Ressource principale : pokemons
# --------------------------------------------------
@dlt.resource(write_disposition="replace")
def pokemons():
    for page in pokeapi_client.paginate("/pokemon"):
        for item in page:
            yield pokeapi_client.get(item["url"]).json()

# --------------------------------------------------
# Ressource paramétrée : abilities
# --------------------------------------------------
@dlt.resource(write_disposition="replace")
def abilities(pokemons):
    seen = set()
    for pokemon in pokemons:
        for ab in pokemon.get("abilities", []):
            url = ab["ability"]["url"]
            if url not in seen:
                seen.add(url)
                yield pokeapi_client.get(url).json()

# --------------------------------------------------
# Ressource paramétrée : moves
# --------------------------------------------------
@dlt.resource(write_disposition="replace")
def moves(pokemons):
    seen = set()
    for pokemon in pokemons:
        for mv in pokemon.get("moves", []):
            url = mv["move"]["url"]
            if url not in seen:
                seen.add(url)
                yield pokeapi_client.get(url).json()

# --------------------------------------------------
# Source DLT regroupant toutes les ressources
# --------------------------------------------------
@dlt.source
def pokeapi_source():
    pokes = pokemons()
    return (
        pokes,
        abilities.bind(pokes),
        moves.bind(pokes),
    )

# --------------------------------------------------
# Pipeline DLT
# --------------------------------------------------
pipeline = dlt.pipeline(
    pipeline_name="pokeapi_full",
    dataset_name="pokemon",
    destination="filesystem",
)

if __name__ == "__main__":
    load_info = pipeline.run(pokeapi_source())
    print(load_info)