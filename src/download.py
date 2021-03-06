from typing import List
import requests
import csv

NOMINA_COLUMNS = [
    "dependencia",
    "nombrePuesto",
    # "nombres",
    # "nombre",
    # "primerApellido",
    # "segundoApellido",
    "informacion_reservada",
    "nivel_tabular_pagado",
    "seg_nal",
    "compensacionGarantizada",
    "sueldoBase",
    "sueldoBruto",
    "sueldoNeto",
    "sueldo_pagado",
    "ramo",
    "ua",
    "idUr",
]

def download_nomina(ramo: int, unidad_responsable:str, start: int, size: int = 100) -> List[dict]:
    fields_to_query = "\n".join([f"      {column}" for column in NOMINA_COLUMNS])
    endpoint = "https://nomina-elastic.apps.funcionpublica.gob.mx/graphql/"
    data = {"operationName": "consultaNominaPorRamoPaginado",
            "variables": {"_ramo": ramo, "_ur": unidad_responsable, "_initial": start, "_size": size},
            "query": """query consultaNominaPorRamoPaginado($_ramo: Int!, $_ur: String, $_initial: Int!, $_size: Int!) {
  consultaNominaPorRamoPaginado(ramo: $_ramo, unidad: $_ur, registroInicio: $_initial, cantidadRegistros: $_size) {
    responseCode
    servPubTotalSector
    listDtoServidorPublicoDto {
""" + fields_to_query + """
    }
  }
}
"""}

    json_response = requests.post(endpoint, json=data).json()

    if "errors" in json_response:
        return None
    return json_response["data"]["consultaNominaPorRamoPaginado"]["listDtoServidorPublicoDto"]




def download_sectores() -> List[dict]:
    sectores_json = requests.get("https://nominatransparente.rhnet.gob.mx/assets/sectores.json").json()
    return [val for _, val in sectores_json.items()]



def download_entidades(ramo: int) -> List[dict]:
    response = requests.post("https://dgti-ejz-entes.200.34.175.120.nip.io/api/graphql",
                             json={"operationName": "obtenerEntes", "variables": {"ramo": ramo},
                                   "query": """query obtenerEntes($ramo: Int!) {
  obtenerEntes(filtro: {ramo: $ramo}) {
    id
    unidadResponsable
    nombreCorto
    enteDesc
  }
}
"""})
    return [data for data in  response.json()['data']['obtenerEntes']]


def escribe_sectores():
    with open("data/raw/sectores.csv", "w", encoding="utf8") as writable:
        writer  = csv.writer(writable)
        writer.writerow(["id", "name"])
        for sector in download_sectores():
            writer.writerow((sector["id"], sector["name"]))

def escribe_entidades():
    with open("data/raw/entidades.csv", "w") as writable:
        writer = csv.writer(writable)
        writer.writerow(("sectorId", "entidadId", "unidadResponsable", "nombreCorto", "nombre"))
        with open("data/raw/sectores.csv") as readable:
            reader = csv.DictReader(readable)
            for sector in reader:
                ramo = sector["id"]
                for entidad in download_entidades(ramo):
                    writer.writerow((
                        ramo,
                        entidad["id"],
                        entidad["unidadResponsable"],
                        entidad["nombreCorto"],
                        entidad["enteDesc"]
                    ))


# escribe_sectores()
# escribe_entidades()

# nomina = download_nomina(ramo=50, unidad_responsable="GYR", start=0, size=100)
