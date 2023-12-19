#!/usr/bin/env python3
"""

Downloads all pictures from MINKA belonging to a certain taxa.

Arguments:
    file: a .txt file with a taxa name per line
    output: name of the output folder

How it works:
    1. Parse all the taxa names from the input file
    2. Look in MINKA for a taxa that matches the input name, get the taxa_id
    3. Look for all observations that have match taxa_id
    4. Loop through all observations and get the picture id
    5. Download ALL pictures

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 18/12/23
"""

from argparse import ArgumentParser
import os
import requests
import rich
from rich.progress import Progress
import json
import numpy as np
from parallelism import threadify

taxa_url = "https://minka-sdg.org:4000/v1/taxa"

def minka_get(endpoint: str, params={}) -> dict:
    """
    HTTP get at a MINKA endpoint
    :param url: base url
    :param endpoint: endpoint
    :param params: dict with parameters
    :return:
    """
    assert (type(endpoint) is str)
    assert (type(params) is dict)

    minka_url = "https://minka-sdg.org:4000/v1/"

    r = requests.get(f"{minka_url}{endpoint}", params=params)
    if r.status_code > 300:
        rich.print(f"[red]HTTP error: code {r.status_code}")
        rich.print(f"[red]Response:{r.text}")
        raise ValueError("Error in HTTP petition to MINKA")
    resp = json.loads(r.text)
    return resp


def minka_get_pagination(endpoint, params={}, per_page=200):
    """
    Similar to minka_get, but it takes into account pagination
    :param endpoint:
    :param params:
    :return:
    """
    i = 1
    params["per_page"] = per_page
    resp = minka_get(endpoint, params=params)  # get first page
    results = resp["results"]
    total = resp['total_results']
    npages = np.ceil(total/per_page) # calculate number of pages

    with Progress() as progress:


        while i < npages:
            i += 1
            params["page"] = i
            resp = minka_get(endpoint, params=params)
            results += resp["results"]

    if len(results) != total:
        rich.print(f"[red]expected {len(results)} but got {total}!")
        raise ValueError("Expected and returned lengths do not match")
    return results

def download_picture(pic_id: int, output: str) -> (bool, str):
    """
    Downloads a picture from MINKA
    :param pics:
    :param output:
    :return: a (success, url) tuple with types (bool, str)
    """
    formats = ["jpeg", "jpg", "png"]
    success = False
    for format in formats:
        url = f"https://minka-sdg.org/attachments/local_photos/files/{pic_id}/original.{format}"
        r = requests.get(url)
        if r.status_code < 300:
            success = True
            break

    if success:
        with open(output, "wb") as f:
            f.write(r.content)

    return success, url

def get_pictures_from_taxa(taxa_id: int, taxa: str) -> (list, dict):
    """
    Returns a list of all pictures that contain a certain taxa

    :param taxa_id: id of the taxa
    :param taxa: scientific name of the taxa
    :param name of the taxa: id of the taxa
    :return: (pic_ids, licenses) where pic_ids is a list and licenses is a dict like pic_id: license
    """
    rich.print(f"Getting all observations for [purple]{taxa}")
    results = minka_get_pagination("observations", params={"taxon_id": taxa_id})
    licenses = {}
    pic_ids = []
    for r in results:  # Loop through results
        license = r["license_code"]
        for p in r["photos"]:  # every result may have several pictures
            pic_ids.append(p["id"])   # append picture ID
            if license:
                licenses[p["id"]] = license # store picture license
            else:
                licenses[p["id"]] = "unknown"

    pic_ids = np.unique(pic_ids)  # avoid duplicated pictures
    return pic_ids, licenses


if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("species", type=str, help="Species txt file", default="")
    argparser.add_argument("output", type=str, help="Output folder", default="")
    args = argparser.parse_args()

    rich.print(f"Loading species from {args.species}...")
    with open(args.species) as f:
        species = f.readlines()

    species = [s.strip() for s in species]
    taxa_ids = {}

    # Normalize names like "Chromis chromis": "chromis_chromis"
    normalized_names = {taxa: taxa.lower().replace(" ", "_").replace(".", "") for taxa in species}

    # Get taxa IDs from MINKA
    for taxa in species:
        taxa_norm = normalized_names[taxa]
        folder = os.path.join(args.output, taxa_norm)
        os.makedirs(folder, exist_ok=True)
        rich.print(f"Getting taxa_id for {taxa}...", end="")
        resp = minka_get("taxa", params={"q": taxa})
        taxa_id = ""
        # Loop until we get an exact match
        for result in resp["results"]:
            if result["name"].lower() == taxa.lower():
                taxa_id = result["id"]
                rich.print(f"[green]{taxa_id}")
                break
        if not taxa_id:
            rich.print(f"[red]not found!")
            continue
        taxa_ids[taxa] = taxa_id

    # Now lets download all pictures
    for taxa, taxa_id in taxa_ids.items():
        taxa_norm = normalized_names[taxa]

        # Find all pictures that contain a certain taxa
        pics, licenses = get_pictures_from_taxa(taxa_id, taxa)
        # Store the pictures according to their license following the pattern:
        #   <folder>/<taxa>/<license>/<pic_id>.jpeg
        arguments = []
        for pic in pics:
            path = os.path.join(args.output, taxa_norm, licenses[pic])
            os.makedirs(path, exist_ok=True)
            filename = os.path.join(path, f"{pic}.jpeg")
            a = (pic, filename)
            arguments.append(a)
        total = len(arguments)
        # Download using Threads
        stats = threadify(arguments, download_picture, text=f"Downloading {len(arguments)} [cyan]{taxa}[/cyan] pics...")

        # Showing some results
        good = len([s for s, _ in stats if s is True])  # number of successful downloads
        bad  = len([s for s, _ in stats if s is False]) # number of failed downloads

        with open(os.path.join(args.output, taxa_norm, "failed.txt"), "w") as f:
            [f.write(url + "\n") for status, url in stats if not status]


        rich.print(f"[green]   {taxa} success: {100*(good/total):.02f} %%")
        rich.print(f"[red]   {taxa} fail: {100*(bad/total):.02f} %%")



