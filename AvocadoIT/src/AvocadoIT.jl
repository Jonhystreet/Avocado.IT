#

module AvocadoIT
    using Pkg,Printf, CSV, JuliaDB, Distributions, OnlineStats, DataFrames, Dates, Plots, PyCall, HTTP, ZipFile, Logging, TerminalLoggers, Dates, XLSX, ExcelReaders
    using CSVFiles
    include("DescargaDatos.jl")
    include("Repositorios.jl")
    include("CombinaDatos.jl")
end
