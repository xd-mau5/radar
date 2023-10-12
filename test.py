import imageio
import yaml
import pyart
import os
import shutil
import timeit
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import datetime
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import datetime
import pandas as pd
import glob
import matplotlib.pyplot as plt

def get_date_utc():
    """
    Returns the current date in UTC timezone as a string in the format 'YYYY/MM/DD'.
    """
    fechaDeHoy = datetime.datetime.now(tz=datetime.timezone.utc)
    fechaDeHoy = pd.to_datetime(fechaDeHoy, format='%Y/%m/%d')
    fechaDeHoy = fechaDeHoy.strftime('%Y/%m/%d')
    return fechaDeHoy

def get_file_list_from_s3(date, radar):
    """
    Returns a list of objects in the S3 bucket 's3-radaresideam' that exceed 1 MB in size, and are located in the
    directory 'l2_data/date/radar', where 'date' is a string in the format 'YYYY/MM/DD', and 'radar' is a string
    representing the name of the radar.

    Parameters:
    date (str): A string representing the date in the format 'YYYY/MM/DD'.
    radar (str): A string representing the name of the radar.

    Returns:
    list: A list of objects in the S3 bucket 's3-radaresideam' that exceed 1 MB in size, and are located in the
    directory 'l2_data/date/radar'.
    """
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    bucket = 's3-radaresideam'
    s3_prefix = 'l2_data/' + date + '/' + radar
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=s3_prefix)
    # Crear una lista de objetos en el bucket de S3 que superen 1 MB de tamaño
    listaDeObjetos = []
    for page in pages:
        for obj in page['Contents']:
            if obj['Size'] > 400000:
                listaDeObjetos.append(obj['Key'])
    # Ordenar la lista de objetos
    listaDeObjetos.sort()
    return listaDeObjetos

def download_files_from_s3(file_list, folder):
    """
    The function `download_files_from_s3` downloads a list of files from an S3 bucket and saves them in
    a specified folder.
    
    :param file_list: The `file_list` parameter is a list of file paths in the S3 bucket that you want
    to download. Each file path should be a string
    :param folder: The `folder` parameter is the directory where you want to download the files from S3.
    It is the destination folder where the downloaded files will be stored
    """
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    if os.path.exists(folder):
        shutil.rmtree(folder)
    os.mkdir(folder)
    for file in file_list:
        s3.download_file('s3-radaresideam', file, folder + '/' + file.split('/')[-1])
        print('Descargando ' + file.split('/')[-1])
    print('Descarga finalizada')

import glob
import os

def get_file_list_from_folder(folder):
    """
    Returns a list of files in a folder that exceed 1 MB in size.

    Parameters:
    folder (str): A string representing the path to the folder.

    Returns:
    list: A list of files in the folder that exceed 1 MB in size.
    """
    listaDeArchivos = glob.glob(folder + '/*')
    listaDeArchivos = [archivo for archivo in listaDeArchivos if os.path.getsize(archivo) > 400000]
    listaDeArchivos.sort()
    return listaDeArchivos

def get_location_from_radar(file):
    """
    Returns the latitude and longitude of the radar.

    Parameters:
    file (str): A string representing the path to the radar file.

    Returns:
    tuple: A tuple containing the latitude and longitude of the radar.
    """
    radar_lat = file.latitude['data'][0]
    radar_lon = file.longitude['data'][0]
    return radar_lat, radar_lon

def get_date_from_radar_to_colombian_time(file):
    """
    The function `get_date_from_radar_to_colombian_time` receives a radar file path and returns the date and time
    of the radar in Colombian time (UTC-5).

    :param file: The `file` parameter is a string representing the path to the radar file.
    :return: The function returns a string representing the date and time of the radar in Colombian time (UTC-5).
    """
    radar = pyart.io.read(file)
    fecha = radar.time['units'][14:]
    fecha = datetime.datetime.strptime(fecha, '%Y-%m-%dT%H:%M:%SZ')
    fecha = fecha - datetime.timedelta(hours=5) # Fix: subtract timedelta from datetime object
    fecha = fecha.strftime('%Y/%m/%d %H:%M:%S')
    return fecha

def get_range_from_radar(file):
    """
    Returns the maximum range of the radar in kilometers.

    Parameters:
    file (str): A string representing the path to the radar file.

    Returns:
    float: The maximum range of the radar in kilometers.
    """
    radar = pyart.io.read(file)
    return radar.range['data'][-1]/1000

def get_radar_name(file):
    """
    Returns the name of the radar.

    Parameters:
    file (str): A string representing the path to the radar file.

    Returns:
    str: The name of the radar.
    """
    name = file.metadata['instrument_name']
    return name

def create_plot(data, var, vmin, vmax, cities_dict):
    """
    The function `create_plot` receives radar data, a variable, minimum and maximum values, and a dictionary of cities
    with their respective latitude and longitude coordinates. It creates a plot of the radar data with the specified
    variable and range, and adds features such as city markers, radar location marker, and range rings. The plot is saved
    as a PNG file with the date and time of the radar data as the filename.

    :param data: The `data` parameter is a pyart radar object representing the radar data.
    :param var: The `var` parameter is a string representing the variable to be plotted.
    :param vmin: The `vmin` parameter is a float representing the minimum value of the colorbar.
    :param vmax: The `vmax` parameter is a float representing the maximum value of the colorbar.
    :param cities_dict: The `cities_dict` parameter is a dictionary representing the cities to be plotted on the map,
                        with their respective latitude and longitude coordinates.

    :return: The function returns a string representing the filename of the saved PNG file.
    """
    display = pyart.graph.RadarMapDisplay(data)
    fig = plt.figure(figsize=(15, 13), dpi=200)
    fecha = data.time['units'][14:]
    fecha = datetime.datetime.strptime(fecha, '%Y-%m-%dT%H:%M:%SZ')
    fecha = fecha - datetime.timedelta(hours=5) # Fix: subtract timedelta from datetime object
    fecha = fecha.strftime('%Y/%m/%d %H:%M:%S')
    ax = plt.axes(projection=ccrs.PlateCarree())
    # Hacer que el mapa sea proyectado en toda el área de la imagen
    ax.set_position([0, 0, 1, 1]) # type: ignore
    # Agregar características del mapa
    ax.add_feature(cfeature.BORDERS, linewidth=0.5) # type: ignore
    ax.add_feature(cfeature.STATES, linewidth=0.5) # type: ignore
    ax.add_feature(cfeature.OCEAN, linewidth=0.5, color='lightgray') # type: ignore
    # Definir el área de visualización del mapa utilizando las coordenadas del radar de Corozal
    ax.set_extent([-79, -72, 6, 12]) # type: ignore
    # Agregar líneas de latitud y longitud, solamente del lado izquierdo y abajo
    ax.gridlines(color='gray', linestyle='--', draw_labels=True) # type: ignore
    # Cargar los datos geoespaciales de los municipios
    cities_lat = [city['lat'] for city in cities_dict.values()]
    cities_lon = [city['lon'] for city in cities_dict.values()]
    # Graficar los marcadores de los municipios
    ax.plot(cities_lon, cities_lat, 'kx', markersize=2.5, transform=ccrs.PlateCarree())
    for city in cities_dict.keys():
        lat = cities_dict[city]['lat']
        lon = cities_dict[city]['lon']
        text_x = lon  # La misma longitud que el marcador
        text_y = lat  # La misma latitud que el marcador

        # Ajusta la posición vertical del texto para que esté encima del marcador
        text_y += 0.013  # Ajusta el valor según tus necesidades

        ax.text(text_x, text_y, city, transform=ccrs.PlateCarree(), fontsize=9, ha='center')
    # Guardar la imagen con el nombre de la fecha del radar
    display.plot_ppi_map(
        var, 
        0,
        vmin=vmin, 
        vmax=vmax,
        resolution='10m', 
        cmap='pyart_NWSRef', 
        colorbar_label='Factor de Reflectividad (dBZ)',
        colorbar_orient='vertical',
        fig=fig,
        filter_transitions=True,
        ax=ax,
        projection=ccrs.PlateCarree(),
        raster=True
        )
    # Titulo de la imagen
    plt.title('Radar Corozal - {} UTC-5'.format(fecha), fontsize=25)
    # Sombrear el área fuera del rango de alcance del radar, llenar con color gris
    display.plot_range_ring(300, 1000, ax=ax, color='k', ls='--', alpha=1)
    # Todo lo que esté fuera del rango de alcance del radar, llenar con color gris
    fig.tight_layout(pad=0, w_pad=0, h_pad=0, rect=(0, 0, 1, 1))
    fecha = fecha.replace('/', '_')
    plt.savefig('{}.png'.format(fecha))
    plt.close()
    # Guardar la imagen con la fecha del radar
    print("Imagen creada con nombre", '{}.png'.format(fecha))
    return '{}.png'.format(fecha)

def create_gif_from_images(image_list, radar):
    """
    Creates a gif file from a list of images.

    Parameters:
    image_list (list): A list of image file paths.
    radar (str): The name of the radar.

    Returns:
    None
    """
    # Generar el gif
    gif_file = radar + '.gif'
    with imageio.get_writer(gif_file, mode='I', duration=200, loop=0) as writer:
        for image in image_list:
            image_data = imageio.v2.imread(image)
            writer.append_data(image_data) # type: ignore
    print('Gif creado con nombre', radar + '.gif')

def delete_files_from_folder(folder):
    """
    Deletes all files in a folder and removes the folder itself.

    Parameters:
    folder (str): The name of the folder to be deleted.

    Returns:
    None
    """
    shutil.rmtree(folder)
    print('Carpeta', folder, 'eliminada')
    images = glob.glob('*.png')
    for image in images:
        os.remove(image)
    print('Imagenes eliminadas')

def main():
    """
    This function is the main function that runs the radar program. It downloads radar data from an S3 bucket, creates a list of files, creates a list of images, creates a dictionary of municipalities, creates a gif file from the images, and deletes the downloaded files.

    Parameters:
    None

    Returns:
    None
    """
    # Definir la fecha de hoy
    fechaDeHoy = get_date_utc()
    # Definir el radar
    radar = 'Corozal'
    # Definir el folder donde se van a descargar los archivos
    folder = 'Corozal'
    # Descargar los archivos del bucket de S3
    print('Lista de archivos en S3')
    lista_s3 = get_file_list_from_s3(fechaDeHoy, radar)
    print(lista_s3)
    download_files_from_s3(get_file_list_from_s3(fechaDeHoy, radar)[-40:], folder)
    # Crear una lista de archivos en el folder
    listaDeArchivos = get_file_list_from_folder(folder)
    print(listaDeArchivos)
    # Crear una lista de imágenes
    listaDeImagenes = []
    print(get_range_from_radar(listaDeArchivos[0]))
    # Crear un diccionario de municipios leídos del archivo YAML
    with open('locations.yaml') as file:
        municipios_dict = yaml.load(file, Loader=yaml.FullLoader)
    # Crear una lista de imágenes
    for file in listaDeArchivos[-37:]:
        radar_data = pyart.io.read(file)
        listaDeImagenes.append(create_plot(radar_data, 'reflectivity', 0, 80, municipios_dict))

    # Crear el gif
    create_gif_from_images(listaDeImagenes, radar)
    # Eliminar los archivos del folder
    delete_files_from_folder(folder)

if __name__ == '__main__':
    print(timeit.timeit("main()", setup="from __main__ import main", number=1), 'segundos')
    
