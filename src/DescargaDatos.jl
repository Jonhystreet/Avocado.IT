#
using Printf, CSV, JuliaDB, Distributions, OnlineStats, DataFrames, Dates, Plots, PyCall, HTTP, ZipFile, Logging, TerminalLoggers, Dates, XLSX, ExcelReaders

export Descargados,Descargable
export covidActual, conevalLink, imLink, natalidad_dat,deflink
export descargar_datos

#Barra de progreso
global_logger(TerminalLogger(right_justify=120))


struct Descargable
            path::String
            fecha::String
end

struct Descargados
            covid::Descargable
            coneval::Descargable
            im::Descargable
            defunciones::Descargable
            natalidad::Descargable
end

struct TablasIM
            uno::DataFrame
            dos::DataFrame
end

#Regresa string URL de descarga
#links_covid(FECHA)
function links_Covid(f="")
            ff = DateFormat("dd-mm-yyyy")
            fe = Date(f,ff)
            if string(Dates.year(fe)) == "2020"
                return covidLink = string("http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/",
                    @sprintf("%02d",Dates.month(fe)),"/datos_abiertos_covid19_",
                    @sprintf("%02d",Dates.day(fe)),".",
                    @sprintf("%02d",Dates.month(fe)),".2020.zip")
            elseif string(Dates.year(fe)) == "2021"
                return covidLink = string("http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/2021/",
                    @sprintf("%02d",Dates.month(fe)),"/datos_abiertos_covid19_",
                    @sprintf("%02d",Dates.day(fe)),".",
                    @sprintf("%02d",Dates.month(fe)),".2021.zip")
            end
end

#Descomprime archivo, regresa la direccion del archivo
#unzip(ARCHIVO_COMPRIMIDO, DIRECCION_DESCOMPRIMIR)
#=
function unzip(rar,pathS="")
    local pathC = ""
    rzip = zipfile.ZipFile(rar)
    for i in rzip.namelist()
        path = joinpath(pathS,i)
        if isempty(pathC)
            pathC = path
        end
        rzip.extract(i, pathS)
    end
    rzip.close()
    rm(rar, force=true, recursive=true)
    return pathC
end
=#

function unzip(file,exdir="")
    fileFullPath = isabspath(file) ?  file : joinpath(pwd(),file)
    basePath = dirname(fileFullPath)
    outPath = (exdir == "" ? basePath : (isabspath(exdir) ? exdir : joinpath(pwd(),exdir)))
    isdir(outPath) ? "" : mkdir(outPath)
    zarchive = ZipFile.Reader(fileFullPath)
    for f in zarchive.files
        fullFilePath = joinpath(outPath,f.name)
        if (endswith(f.name,"/") || endswith(f.name,"\\"))
            mkdir(fullFilePath)
        else
            write(fullFilePath, read(f))
        end
    end
    close(zarchive)
    return fileFullPath
end

covidActual = "http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/datos_abiertos_covid19.zip"
#Descarga archivo covid, regresa la dirrecion del archivo
#descargar_covid(DIRECCION,FECHA), si no se especifica DIRECCION el archivo se guarda en la carpeta actual, si no se especifica FECHA se obtienen los datos mas actualizados
#FECHA formato dia-mes-año ejem."15-1-2020"
function descargar_covid(path="",f="")
            if isempty(path)
                path = pwd()
            end
            if isempty(f)
                covidLink = covidActual
                f = string(today())
            else
                covidLink = links_Covid(f)
            end
            pathC = joinpath(path,"covid")
            rm(pathC, force=true, recursive=true)
            mkdir(pathC)
            rar = HTTP.download(covidLink, pathC)
            return Descargable(unzip(rar, pathC),f)
end

#Datos de CONEVAL 2008 al 2018
conevalLink = "https://www.coneval.org.mx/Medicion/MP/Documents/Pobreza_18/AE_nacional_estatal_2008_2018.zip"
#Descarga los datos relevantes del CONEVAL, regresa direccion del archivo
#descargar_coneval(DIRECCION), si no se especifica DIRECCION el archivo se guarda en la carpeta actual
function descargar_coneval(path="",f="")
            if isempty(path)
                path = pwd()
            end
            if isempty(f)
                f = string(today())
            end
            pathC = joinpath(path,"coneval")
            rm(pathC, force=true, recursive=true)
            mkdir(pathC)
            rar = HTTP.download(conevalLink, pathC)
            #return Descargable(unzip(rar, pathC),f)
end

#Datos intensidad migratoria por municipios, 2010
imLink = "http://www.omi.gob.mx/work/models/OMI/Resource/538/1/images/IIM2010_BASEMUN.xls"

#Descarga los datos del Observatorio de Migración Internacional, regresa direccion del archivo
#descargar_im(DIRECCION), si no se especifica DIRECCION el archivo se guarda en la carpeta actual
function descargar_im(path="",f="")
            if isempty(path)
                path = pwd()
            end
            if isempty(f)
                f = string(today())
            end
            pathC = joinpath(path,"Intensidad Migratoria")
            rm(pathC, force=true, recursive=true)
            mkdir(pathC)
            rar = HTTP.download(imLink, pathC)
            return Descargable(rar,f)
end
#Descarga los datos de la fecundacion y nacimientos a nivel nacional en mexico

natalidad_dat= "http://www.conapo.gob.mx/work/models/CONAPO/Datos_Abiertos/Proyecciones2018/tef_nac_proyecciones.csv"

function descargar_natalidad(path="",f="")
            if isempty(path)
                path = pwd()
            end
            if isempty(f)
                f = string(today())
            end
            pathC = joinpath(path,"Natalidad")
            rm(pathC, force=true, recursive=true)
            mkdir(pathC)
            rar = HTTP.download(natalidad_dat, pathC)
            return Descargable(rar,f)
end

#descargar_defunciones
deflink = "http://www.conapo.gob.mx/work/models/CONAPO/Datos_Abiertos/Proyecciones2018/def_edad_proyecciones_n.csv"
#Descarga los datos relevantes del conapo, regresa direccion del archivo
#descargar_defunciones(DIRECCION), si no se especifica DIRECCION el archivo se guarda en la carpeta actual
function descargar_defunciones(path="",f="")
            if isempty(path)
                path = pwd()
            end
            if isempty(f)
                f = string(today())
            end
            pathC = joinpath(path,"Defunciones")
            rm(pathC, force=true, recursive=true)
            mkdir(pathC)
            rar = HTTP.download(deflink, pathC)
            return Descargable(rar,f)
end

#Combina las funciones de descarga
#descargar_datos(DIRECCION,FECHA), si no se especifica DIRECCION se ocupa la carpeta actual
function descargar_datos(path="",f="")
            if isempty(path)
                path = pwd()
            end
            pathC = joinpath(path,"Descargables")
            rm(pathC, force=true, recursive=true)
            mkdir(pathC)
            uno = descargar_covid(pathC,f)
            dos = descargar_coneval(pathC,f)
            tres = descargar_im(pathC,f)
            cuatro = descargar_defunciones(pathC,f)
            cinco = descargar_natalidad(pathC,f)
            #return Descargados(uno, dos, tres, cuatro ,cinco)
end
