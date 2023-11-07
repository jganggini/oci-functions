import io
import oci
import pathlib
import random
import time
import asyncio
from tabulate import tabulate

################################
#           Parameter          #
################################
# [Parameter:utl] Utility
utl_path                 = str(pathlib.Path(__file__).parent.absolute())
# [Parameter:oci]
par_oci_config           = oci.config.from_file(utl_path + '/_oci/config')
par_oci_object_storage   = oci.object_storage.ObjectStorageClient(par_oci_config)
# [Parameter:oci_obj] OCI Object Storage
par_oci_obj_bucket_name  = 'DLK1LAGDEV'
par_oci_obj_folders      = ['iops-data-for-analytics-[1MB-100MB]',
                            'iops-data-for-data-lake-[10MB-1GB]',
                            'iops-data-for-transactional-processes-[100kB-10MB]']
num_iterations = 100

# Función para listar objetos en carpetas de un bucket, tomar uno de forma aleatoria y calcular los IOPS en 100 iteraciones por cada carpeta
async def list_select_random_objects_and_calculate_iops(folder, object_storage, num_iterations):
    total_download_time = 0
    object_names = []

    for _ in range(num_iterations):
        # Listar objetos en el folder del bucket
        list_objects_response = par_oci_object_storage.list_objects(object_storage.get_namespace().data, par_oci_obj_bucket_name, prefix=folder)

        # Obtener la lista de nombres de objetos en el folder (sin el nombre del folder)
        object_names = [obj.name.split("/")[-1] for obj in list_objects_response.data.objects]

        if not object_names:
            return {"message": "La carpeta está vacía"}

        # Seleccionar aleatoriamente un nombre de objeto en el folder
        random_object_name = folder + "/" + random.choice(object_names)

        start_time = time.time()
        object_storage.get_object(object_storage.get_namespace().data, par_oci_obj_bucket_name, random_object_name)
        end_time = time.time()

        total_download_time += end_time - start_time

    average_download_time = total_download_time / num_iterations
    average_iops = 1 / average_download_time

    return {"folder": folder, "average_iops": average_iops}

# Función principal para realizar 100 iteraciones en paralelo
async def amain():
    tasks = []

    for folder in par_oci_obj_folders:
        task = list_select_random_objects_and_calculate_iops(folder, par_oci_object_storage, num_iterations)
        tasks.append(task)

    results = await asyncio.gather(*tasks)
    
    # Calcular el promedio de IOPS por folder
    folder_iops = {}
    for result in results:
        folder = result["folder"]
        average_iops = result["average_iops"]
        if folder in folder_iops:
            folder_iops[folder].append(average_iops)
        else:
            folder_iops[folder] = [average_iops]

    average_iops_by_folder = {}
    for folder, iops_list in folder_iops.items():
        average_iops_by_folder[folder] = sum(iops_list) / len(iops_list)

    # Imprimir los resultados de IOPS en formato de tabla
    table = [(folder, average_iops) for folder, average_iops in average_iops_by_folder.items()]
    print(tabulate(table, headers=["Folder", "IOPS Promedio " + str(num_iterations)], tablefmt="fancy_grid"))

if __name__ == '__main__':
    asyncio.run(amain())