import zipfile
import os

def unzip_file(file_dir, destination_dir):
    '''
    Utility function to unzip and save a file in a specified dir.

    Parameters
    ----------
        file_dir (str): path to the zip file
        destination_dir (str): path to the destination directory
    '''
    try:
        with zipfile.ZipFile(file_dir, 'r') as zip_obj:
            for file_name in zip_obj.namelist():
                # Check for path traversal
                abs_path = os.path.abspath(os.path.join(destination_dir, file_name))
                if not abs_path.startswith(os.path.abspath(destination_dir)):
                    raise Exception(f'Unsafe file detected in zip: {file_name}')
                else:
                    # Extract all files to destination directory
                    zip_obj.extractall(destination_dir)
                    print(f'All files saved in: {destination_dir}')
        
    except zipfile.BadZipFile:
        print(f'Error: {file_dir} is not a valid zip file.')

