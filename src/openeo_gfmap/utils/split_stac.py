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
    for item in collection.get_all_items():
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

        new_item_assets_extension = (
            pystac.extensions.item_assets.ItemAssetsExtension.ext(
                new_collection, add_if_missing=True
            )
        )

        new_item_assets_extension.item_assets = item_assets_extension.item_assets
    return new_collection


def split_collection_by_epsg(
    collection: Union[str, Path, pystac.Collection], output_dir: Union[str, Path]
):
    """
    Split a STAC collection into multiple STAC collections based on EPSG code.

    Parameters
    ----------
    collection: Union[str, Path, pystac.Collection]
        A collection of STAC items or a path to a STAC collection.
    output_dir: Union[str, Path]
        The directory where the split STAC collections will be saved.
    """

    if not isinstance(collection, pystac.Collection):
        collection = Path(collection)
        output_dir = Path(output_dir)
        os.makedirs(output_dir, exist_ok=True)

        try:
            collection = pystac.read_file(collection)
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
