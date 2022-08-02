
import glob
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import time
import pickle
import numpy as np
import rasterio as rio
from rasterio.enums import Resampling
import boto3
from botocore.exceptions import ClientError
import io
import rioxarray
def checkBackDate():
    no_back = 5
    backDate = datetime.today() - timedelta(days=no_back)
    backDate = str(backDate.date()).replace("-","")
    today = str(datetime.today().date()).replace("-","")
    return today, backDate

def getListDate(today:str, backDate:str):
    list_date = pd.date_range(start=backDate, end=today)
    list_date = [str(i.date()).replace("-","") for i in list_date]
    return list_date

def getListFile(fullpath,tile):
    list_file = glob.glob(fullpath+"".join(tile)+"/*")
    list_file = [i.split("/")[-1] for i in list_file]
    print(list_file)
    return list_file

def checkDownload(list_file,date):
    if date in list_file:
        return True
    else:
        return False

def getListDownload(date, tile):
    fullpath =  "/fs1/"
    outputFolder = f'{fullpath}{tile[0]}{tile[1]}{tile[2]}/{date}'

    list_file = getListFile(fullpath,tile)
    haveFile = checkDownload(list_file, date)
    if haveFile == False:
        B2 = f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0]}/{tile[1]}/{tile[2]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R10m/*B02*.jp2' {outputFolder}/{date}_B02.jp2"
        B3 = f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0]}/{tile[1]}/{tile[2]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R10m/*B03*.jp2' {outputFolder}/{date}_B03.jp2"
        B4 = f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0]}/{tile[1]}/{tile[2]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R10m/*B04*.jp2' {outputFolder}/{date}_B04.jp2"
        B8 = f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0]}/{tile[1]}/{tile[2]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R10m/*B08*.jp2' {outputFolder}/{date}_B08.jp2"
        Btci = f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0]}/{tile[1]}/{tile[2]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R10m/*TCI*.jp2' {outputFolder}/{date}_TCI.jp2"
        Bscl = f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0]}/{tile[1]}/{tile[2]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R20m/*SCL*.jp2' {outputFolder}/{date}_SCL.jp2"
        Bmeta = f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0]}/{tile[1]}/{tile[2]}/S2*_MSIL2A_{date}*/MTD_MSIL2A.xml' {outputFolder}/{date}_meta.xml"
        list_download = [B2,B3,B4,B8,Btci,Bscl,Bmeta]
        return list_download
    else:
        list_download = []
        return list_download

def calculateNDVI(B8:str, B4:str):

     try:
        np.seterr(invalid='ignore')
        B8_src = rio.open(B8)
        B4_src = rio.open(B4)

        B8_arr = B8_src.read(1)
        B4_arr = B4_src.read(1)

        ndvi = (B8_arr.astype(np.float32)-B4_arr.astype(np.float32))/(B8_arr+B4_arr)
        ndvi_src = B4_src.profile
        print(B4, B8)
        print(ndvi_src)
        ndvi_src.update(count=1,compress='lzw', driver="GTiff",dtype=rio.float32)

        B8_src.close()
        B4_src.close()
        
        return ndvi_src, ndvi

     except:
        print("Read Image Error B8 :", B8, datetime.today())
        print("Read Image Error B4 :", B4, datetime.today())


def calculateNDWI(B8:str, B3:str):

     try:
        np.seterr(invalid='ignore')
        B8_src = rio.open(B8)
        B3_src = rio.open(B3)

        B8_arr = B8_src.read(1)
        B3_arr = B3_src.read(1)

        ndvi = (B3_arr.astype(np.float32)-B8_arr.astype(np.float32))/(B8_arr+B3_arr)
        ndvi_src = B3_src.profile
        print(B3, B8)
        print(ndvi_src)
        ndvi_src.update(count=1,compress='lzw', driver="GTiff",dtype=rio.float32)

        B8_src.close()
        B4_src.close()
        
        return ndvi_src, ndvi

     except:
        print("Read Image Error B8 :", B8, datetime.today())
        print("Read Image Error B4 :", B3, datetime.today())
def download_sentinel_scene(ti):
    current, backdate = checkBackDate()
    list_date = getListDate(current, backdate)
    tile = ["54","S","VE"]
    fullpath =  f"/fs1/{tile[0]}{tile[1]}{tile[2]}/*"

    pickle.dump(list_date,open('/fs1/date_temp.pkl', 'wb'))

    for i in list_date:
        list_i = getListDownload(i,tile)
        print(list_i)
        print("List of Download : ", len(list_i))
        if len(list_i) != 0:
            for j in list_i:
                os.system(f"{j}")

    downloaded_list = [os.path.basename(i) for i in glob.glob(fullpath)]
    diff_list = list(set(list_date).intersection(set(downloaded_list)))
    print(diff_list)
    ti.xcom_push(key='diff_list', value=[diff_list,tile]) #[[["ddd","ccc"],["46",""s","vc"]]]


def processNDVI(ti):
    value = ti.xcom_pull(key="diff_list", task_ids=["download_s2"])
    print(type(value))
    print("len is ", len(value[0]))
    print(value[0])

    if len(value[0][0])!= 0:
        date_list = value[0][0]
        tile = value[0][1]
        fullpath =  f"/fs1/{tile[0]}{tile[1]}{tile[2]}"
        print("fullpath ",fullpath)

        if not os.path.exists(fullpath+"_NDVI"):
            os.mkdir(fullpath+"_NDVI")

        print("fullpath NDVI : ",fullpath+"_NDVI" )
        print("date list", date_list)

        for date_i in date_list:
            b8 = fullpath+f"/{date_i}/{date_i}_B08.jp2"
            b4 = fullpath+f"/{date_i}/{date_i}_B04.jp2"
            print(b8)
            print(b4)
            ndvi_src, ndvi = calculateNDVI(b8, b4)
            print(type(ndvi))

            if not os.path.exists(fullpath+"_NDVI"+f"/{date_i}"):
                os.mkdir(fullpath+"_NDVI"+f"/{date_i}")

            print("write image filename ",fullpath+"_NDVI/"+f"{date_i}/{date_i}_NDVI.tiff")

            with rio.open(fullpath+"_NDVI/"+f"{date_i}/{date_i}_NDVI.tiff","w",**ndvi_src) as dst:
                dst.write(ndvi.astype(rio.float32),1)
                print("Write successfully")

def processNDWI(ti):
    value = ti.xcom_pull(key="diff_list", task_ids=["download_s2"])
    print(type(value))
    print("len is ", len(value[0]))
    print(value[0])

    if len(value[0][0])!= 0:
        date_list = value[0][0]
        tile = value[0][1]
        fullpath =  f"/fs1/{tile[0]}{tile[1]}{tile[2]}"
        print("fullpath ",fullpath)

        if not os.path.exists(fullpath+"_NDWI"):
            os.mkdir(fullpath+"_NDWI")

        print("fullpath NDWI : ",fullpath+"_NDWI" )
        print("date list", date_list)

        for date_i in date_list:
            b8 = fullpath+f"/{date_i}/{date_i}_B08.jp2"
            b3 = fullpath+f"/{date_i}/{date_i}_B03.jp2"
            print(b8)
            print(b3)
            ndwi_src, ndwi = calculateNDWI(b8, b3)
            print(type(ndwi))

            if not os.path.exists(fullpath+"_NDWI"+f"/{date_i}"):
                os.mkdir(fullpath+"_NDWI"+f"/{date_i}")

            print("write image filename ",fullpath+"_NDWI/"+f"{date_i}/{date_i}_NDWI.tiff")

            with rio.open(fullpath+"_NDWI/"+f"{date_i}/{date_i}_NDWI.tiff","w",**ndwi_src) as dst:
                dst.write(ndwi.astype(rio.float32),1)
                print("Write successfully")


def removeCloud(scl, band, bandname,  output_folder, tile, date):

    if not os.path.exists(f"{output_folder}"):
        os.mkdir(f"{output_folder}")

    if not os.path.exists(f"{output_folder}/{tile}"):
        os.mkdir(f"{output_folder}/{tile}")

    if not os.path.exists(f"{output_folder}/{tile}/{date}"):
        os.mkdir(f"{output_folder}/{tile}/{date}")

    scl_src = rio.open(scl)
    band_src = rio.open(band)
    #scl_img = scl_src.read(1) #keep scl only 4,5 6
    band_img = band_src.read()
    band_img = band_img.astype(np.float16)
    meta = band_src.profile
    data = scl_src.read(out_shape=(scl_src.count,
                                        int(scl_src.height*2), int(scl_src.width*2)),
                                        resampling=Resampling.nearest)
    transform = scl_src.transform*scl_src.transform.scale((scl_src.width/data.shape[-1]), 
                                                        (scl_src.height/data.shape[-2]))

    dataArr = []                                      
    if band_src.count > 1:
        dataArr.append(np.vstack((data,data,data)))
        print("more than 1 band ", dataArr[0].shape)
    else:
        dataArr.append(data)
        print("1 band ", dataArr[0].shape)


    band_img[(dataArr[0]<4)|(dataArr[0]>=7)] = np.nan
    band_img = band_img.astype(np.float16)
    meta.update(dtype=rio.float32, nodata=np.nan,compress='lzw')
    print("cloud removed Image processed")

    scl_src.close()
    band_src.close()

    #write output file
    output_band = f"{output_folder}/{tile}/{date}/{date}_{bandname}.tif"
    print(output_band)
    with rio.open(output_band,"w",**meta) as dst:
            dst.write(band_img.astype(rio.float32))
            print("successfully write")

def processCloudRemove_ndvi(ti):
    value = ti.xcom_pull(key="diff_list", task_ids=["download_s2"])
    print(type(value))
    print("len is ", len(value[0]))
    print(value[0])

    if len(value[0][0])!= 0:
        date_list = value[0][0]
        tile = value[0][1]

        fullpath =  f"/fs1/{tile[0]}{tile[1]}{tile[2]}"
        print("fullpath ",fullpath)

        for date_i in date_list:
            scl = fullpath+f"/{date_i}/{date_i}_SCL.jp2"
            ndvi = fullpath+"_NDVI"+f"/{date_i}/{date_i}_NDVI.tiff"
            print(scl)
            print(ndvi)
            output_folder = f"/fs1/RC"
            removeCloud(scl,ndvi,"NDVI",output_folder, "".join(tile), date_i)
            print("cloud removed write image Done")

def processCloudRemove_ndwi(ti):
    value = ti.xcom_pull(key="diff_list", task_ids=["download_s2"])
    print(type(value))
    print("len is ", len(value[0]))
    print(value[0])

    if len(value[0][0])!= 0:
        date_list = value[0][0]
        tile = value[0][1]

        fullpath =  f"/fs1/{tile[0]}{tile[1]}{tile[2]}"
        print("fullpath ",fullpath)

        for date_i in date_list:
            scl = fullpath+f"/{date_i}/{date_i}_SCL.jp2"
            ndwi = fullpath+"_NDWI"+f"/{date_i}/{date_i}_NDWI.tiff"
            print(scl)
            print(ndwi)
            output_folder = f"/fs1/RC"
            removeCloud(scl,ndwi,"NDWI",output_folder, "".join(tile), date_i)
            print("cloud removed write image Done")


def cld_free_rgb(ti):
    """
    NOTE: This function is used to remove noise such as cloud and darkness pixels.
    20 meter SCL and multispectral band(red & nir) provide cld free bands.
    """
    xds = rioxarray.open_rasterio(fullpath+f"/{date_i}/{date_i}_SCL.jp2")
    new_width = xds.rio.width * 2
    new_height = xds.rio.height * 2
    xds_upsampled = xds.rio.reproject(
        xds.rio.crs,
        shape=(new_height, new_width),
        resampling=Resampling.bilinear,
    )
    scl_upsample=xds_upsampled
    img1=rioxarray.open_rasterio(fullpath+f"/{date_i}/{date_i}_TCI.jp2")[0]
    img2=rioxarray.open_rasterio(fullpath+f"/{date_i}/{date_i}_TCI.jp2")[1]
    img3=rioxarray.open_rasterio(fullpath+f"/{date_i}/{date_i}_TCI.jp2")[2]
    imgscl=scl_upsample[0]
    img1[(imgscl<4)|(imgscl>=7)] = np.nan
    img2[(imgscl<4)|(imgscl>=7)] = np.nan
    img3[(imgscl<4)|(imgscl>=7)] = np.nan
    bands = ['B', 'G', 'R']
    xr_dataset = xr.Dataset()
    xr_dataset[bands[0]] = img1
    xr_dataset[bands[1]] = img2
    xr_dataset[bands[2]] = img3
    output_folder=f"/fs1/RC"
    bandname='tci_cldrm'
    value = ti.xcom_pull(key="diff_list", task_ids=["download_s2"])
    print(type(value))
    print("len is ", len(value[0]))
    print(value[0])
    if len(value[0][0])!= 0:
        date_list = value[0][0]
        tile = value[0][1]
    output_band = f"{output_folder}/{tile}/{date_i}/{date_i}_{bandname}.tif"
    xr_dataset.where(xr_dataset!=0,np.nan).rio.to_raster(output_band)

# def cmap_indicator(ndvi_dst,ndvi_col,ndvi_cmap_dst):
#     ndvi_dst=
#     ndwi_dst=
#     ndvi_col='/fs1/color_nsi.txt'
#     ndwi_col='/fs1/color_nsi_ndwi.txt'
#     ndvi_cmap_dst='/fs1/ndvi.tif'
#     ndwi_cmap_dst='/fs1/ndwi.tif'
#     os.system("gdaldem color-relief -nearest_color_entry -alpha {} {} {}".format(ndvi_dst,ndvi_col,ndvi_cmap_dst))
#     os.system("gdaldem color-relief -nearest_color_entry -alpha {} {} {}".format(ndwi_dst,ndwi_col,ndwi_cmap_dst))
#     print("generate_cmap_index")
def uploadS3(bucket, access, secret,input_filename, output_filename):
    s3_client = boto3.client("s3",region_name="ap-northeast-1",aws_access_key_id=access,aws_secret_access_key=secret)
    try:
        s3_client.upload_file(input_filename, bucket, output_filename)
    except Exception as e:
        print("Error occuured ", e)

def getAwsdata(path):
    bucket = ""
    user = ""
    access = ""
    secret =""

    with open(path,"r") as awsuser:
        data = awsuser.readlines()
        bucket = data[0].replace("\n","").split("=")[1]
        user = data[1].replace("\n","").split("=")[1]
        access = data[2].replace("\n","").split("=")[1]
        secret = data[3].replace("\n","").split("=")[1]
        
    return (bucket, user, access, secret)

def processUploadS3(ti):
    value = ti.xcom_pull(key="diff_list", task_ids=["download_s2"])
    bucket, user, access, secret = getAwsdata("/fs1/s3_user.txt")
    print("bucket is ",bucket)
    print(type(value))
    print("len is ", len(value[0]))
    print(value[0])
    indicator=['NDVI','NDWI','tci_cldrm']
    for inx in indicator:
        if len(value[0][0])!= 0:
            date_list = value[0][0]
            tile = value[0][1]
            fullpath =  f"/fs1/RC/{tile[0]}{tile[1]}{tile[2]}"
            print("fullpath ",fullpath)

            for date_i in date_list:
                input_filename = fullpath+f"/{date_i}/{date_i}_{}.tif".format(inx)
                print("INput Filename  ",input_filename)
                output_filename = f"sprectral_index/NDVI/{tile[0]}{tile[1]}{tile[2]}/{date_i}/{date_i}_{}.tif".format(inx)
                print("Output filename ", output_filename)
                uploadS3(bucket, access, secret,input_filename, output_filename)
                print("Upload to S3 completely")

with DAG("NSI_download_s2",start_date=datetime(2022,6,21),schedule_interval="@daily", catchup=False) as dag:
     download1 = PythonOperator(task_id="download_s2",python_callable=download_sentinel_scene)
     ndviProcess = PythonOperator(task_id="process_ndvi", python_callable=processNDVI)
     ndwiProcess = PythonOperator(task_id="process_ndwi", python_callable=processNDWI)
     removeCloudProcess_ndvi = PythonOperator(task_id="process_removeCloud_ndvi",python_callable=processCloudRemove_ndvi)
     removeCloudProcess_ndwi = PythonOperator(task_id="process_removeCloud_ndwi",python_callable=processCloudRemove_ndwi)
     uploadS3Process = PythonOperator(task_id="process_uploadS3",python_callable=processUploadS3)

download1 >> ndviProcess >> ndwiProcess >> removeCloudProcess_ndvi >> removeCloudProcess_ndwi >> cld_free_rgb >> uploadS3Process
