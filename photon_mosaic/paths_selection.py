from pathlib import Path

import datashuttle as ds


def find_raw_data_paths(base_path):
    errors = ds.datashuttle_functions.quick_validate_project(base_path)

    if not errors:
        base_path = Path(base_path)
        raw_data_paths = sorted(
            base_path.rglob("rawdata/sub-*/ses-*/funcimg/*.tif")
        )
        #  filter
        return raw_data_paths

    else:
        # logger.log(errors)

        raise Exception("Not neuroblueprint compliant", errors)


def adapt_paths_to_output_pattern(all_selected_tiff_paths, output_pattern):
    output_list = []
    for file_path in all_selected_tiff_paths:
        edited_path = str(file_path).replace(".", f"{output_pattern}.", 1)
        edited_path = edited_path.replace("rawdata", "derivatives")
        output_list.append(edited_path)

    return output_list
