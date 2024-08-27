"""Utility function to split a STAC collection into multiple STAC collections based on CRS.
Requires the "proj:epsg" property to be present in all the STAC items.
"""

import os
from pathlib import Path
from typing import Iterator, Union

import pystac


def _extract_epsg_from_stac_item(stac_item: pystac.Item) -> int:
    """
    Extract the EPSG code from a STAC item.

    Parameters:
    stac_item (pystac.Item): The STAC item.

    Returns:
    int: The EPSG code.

    Raises:
    KeyError: If the "proj:epsg" property is missing from the STAC item.
    """

    try:
        epsg_code = stac_item.properties["proj:epsg"]
        return epsg_code
    except KeyError:
        raise KeyError("The 'proj:epsg' property is missing from the STAC item.")


def _get_items_by_epsg(
    collection: pystac.Collection,
) -> Iterator[tuple[int, pystac.Item]]:
    """
    Generator function that yields items grouped by their EPSG code.

    Parameters:
    collection (pystac.Collection): The STAC collection.

    Yields:
    tuple[int, pystac.Item]: EPSG code and corresponding STAC item.
    """
    for item in collection.get_items():
        epsg = _extract_epsg_from_stac_item(item)
        yield epsg, item


def _create_collection_skeleton(
    collection: pystac.Collection, epsg: int
) -> pystac.Collection:
    """
    Create a skeleton for a new STAC collection with a given EPSG code.

    Parameters:
    collection (pystac.Collection): The original STAC collection.
    epsg (int): The EPSG code.

    Returns:
    pystac.Collection: The skeleton of the new STAC collection.
    """
    new_collection = pystac.Collection(
        id=f"{collection.id}_{epsg}",
        description=f"{collection.description} Containing only items with EPSG code {epsg}",
        extent=collection.extent.clone(),
        summaries=collection.summaries,
        license=collection.license,
        stac_extensions=collection.stac_extensions,
    )
    if "item_assets" in collection.extra_fields:
        item_assets_extension = pystac.extensions.item_assets.ItemAssetsExtension.ext(
            collection
        )
        item_assets_extension.add_to(new_collection)
    return new_collection


def _create_item_by_epsg_dict(collection: pystac.Collection) -> dict:
    """
    Create a dictionary that groups items by their EPSG code.

    Parameters:
    collection (pystac.Collection): The STAC collection.

    Returns:
    dict: A dictionary that maps EPSG codes to lists of items.
    """
    # Dictionary to store items grouped by their EPSG codes
    items_by_epsg = {}

    # Iterate through items and group them
    for item in collection.get_items():
        epsg = _extract_epsg_from_stac_item(item)
        if epsg not in items_by_epsg:
            items_by_epsg[epsg] = []
        items_by_epsg[epsg].append(item)

    return items_by_epsg


def _create_new_epsg_collection(
    epsg: int, items: list, collection: pystac.Collection
) -> pystac.Collection:
    """
    Create a new STAC collection with a given EPSG code.

    Parameters:
    epsg (int): The EPSG code.
    items (list): The list of items.
    collection (pystac.Collection): The original STAC collection.

    Returns:
    pystac.Collection: The new STAC collection.
    """
    new_collection = collection.clone()
    new_collection.id = f"{collection.id}_{epsg}"
    new_collection.description = (
        f"{collection.description} Containing only items with EPSG code {epsg}"
    )
    new_collection.clear_items()
    for item in items:
        new_collection.add_item(item)

    new_collection.update_extent_from_items()

    return new_collection


def _create_collection_by_epsg_dict(collection: pystac.Collection) -> dict:
    """
    Create a dictionary that groups collections by their EPSG code.

    Parameters:
    collection (pystac.Collection): The STAC collection.

    Returns:
    dict: A dictionary that maps EPSG codes to STAC collections.
    """
    items_by_epsg = _create_item_by_epsg_dict(collection)
    collections_by_epsg = {}
    for epsg, items in items_by_epsg.items():
        new_collection = _create_new_epsg_collection(epsg, items, collection)
        collections_by_epsg[epsg] = new_collection

    return collections_by_epsg


def _write_collection_dict(collection_dict: dict, output_dir: Union[str, Path]):
    """
    Write the collection dictionary to disk.

    Parameters:
    collection_dict (dict): The dictionary that maps EPSG codes to STAC collections.
    output_dir (str): The output directory.
    """
    output_dir = Path(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    for epsg, collection in collection_dict.items():
        collection.normalize_hrefs(os.path.join(output_dir, f"collection-{epsg}"))
        collection.save()


def split_collection_by_epsg(path: Union[str, Path], output_dir: Union[str, Path]):
    """
    Split a STAC collection into multiple STAC collections based on EPSG code.

    Parameters:
    path (str): The path to the STAC collection.
    output_dir (str): The output directory.
    """
    # path = Path(path)
    # try:
    #     collection = pystac.read_file(path)
    # except pystac.STACError:
    #     print("Please provide a path to a valid STAC collection.")
    # collection_dict = _create_collection_by_epsg_dict(collection)
    # _write_collection_dict(collection_dict, output_dir)

    path = Path(path)
    output_dir = Path(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    try:
        collection = pystac.read_file(path)
    except pystac.STACError:
        print("Please provide a path to a valid STAC collection.")
        return

    collections_by_epsg = {}

    for epsg, item in _get_items_by_epsg(collection):
        if epsg not in collections_by_epsg:
            collections_by_epsg[epsg] = _create_collection_skeleton(collection, epsg)

        # Add item to the corresponding collection
        collections_by_epsg[epsg].add_item(item)

    # Write each collection to disk
    for epsg, new_collection in collections_by_epsg.items():
        new_collection.update_extent_from_items()  # Update extent based on added items
        collection_path = output_dir / f"collection-{epsg}"
        new_collection.normalize_hrefs(str(collection_path))
        new_collection.save()
