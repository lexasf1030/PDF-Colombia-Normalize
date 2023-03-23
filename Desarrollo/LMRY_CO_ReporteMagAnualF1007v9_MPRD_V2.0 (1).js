/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_ReporteMagAnualF1007v9_MPRD_V2.0.js      ||
||                                                              ||
||  Version Date         Author        Remarks                  ||
||  2.0     May 24 2021  LatamReady    Use Script 2.0           ||
\= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.0
 * @NScriptType MapReduceScript
 * @NModuleScope Public
 */
define(["N/record", "N/runtime", "N/file", "N/email", "N/search", "N/format", "N/currency",
        "N/log", "N/config", "N/task", "N/encode", "./CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js"
    ],

    function(record, runtime, file, email, search, format, currency, log, config, task, encode, libreria) {

        var objContext = runtime.getCurrentScript();

        // Parametros del Schedule
        var paramSubsidiaria;
        var paramPeriodo;
        var paramMultibook;
        var paramCont;
        var paramBucle;
        var paramIdReport;
        var paramIdLog;
        var paramIdFeatureByVersion;
        var paramFinBusqueda;
        var paramFinBusquedaJournal;
        var paramConcepto;
        var paramCuantiasMenoresFile;
        var paramIngresosRecibidosFile;
        var param_detallado = false;
        var formato_stardate = '';
        var formato_enddate = '';

        // CONSTANTES
        var CANT_REGISTROS = 85;
        var MAX_REGISTROS_REPORTE = 5000;
        var FILE_SIZE = 7340032;
        var CUANTIA_MINIMA = 500000;

        // Features del Ambiente
        var hasSubsidiariaFeature;
        var hasMultibookFeature;
        var hasJobFeature;
        var isAdvanceJobsFeature;

        var valorTotal = 0;

        // Datos de la Subsidiaria Seleccinada
        var companyName = null;
        var companyRuc = null;

        var multibookName;

        // Datos del Periodo Seleccionado
        var periodEndDate = null;
        var periodStartDate = null;
        var periodName;

        var strExcelIngresosRecibidos = "";
        var strXmlIngRec = "";
        var strCuantiasMenores = "";

        var numeroEnvio = 0;
        var generarXml = false;
        var contarRegistros = 0;

        var currenciesJson = {};
        var vectorIngresos = [];
        var vectorCuantiaMenor = [];
        var paisesArray = [];
        var continuarEjecucion = true;

        var LMRY_script = 'LMRY_CO_ReporteMagAnualF1007v9.0_MPRD_V2.0js';

        var language = runtime.getCurrentScript().getParameter({
            name: 'LANGUAGE'
        }).substring(0, 2);

        var GLOBAL_LABELS = getGlobalLabels();
        /**
         * Input Data for processing
         *
         * @return Array,Object,Search,File
         *
         * @since 2016.1
         */
        function getInputData() {
            try {
                ObtenerParametrosYFeatures();
                ObtenerDatosSubsidiaria();

                //Obtener Monedas y Cuantia menor
                if (hasSubsidiariaFeature && hasMultibookFeature) {
                    ObtenerCurrencies();
                }
                ObtenerPaises();
                log.debug('paisesArray', paisesArray);
                //Se obtienen los vectores de ingresos vectorIngresos y vectorCuantiaMenor, Transacciones con customer y Journals sin customer asignado
                var vector_ingresos = ObtieneIngresos1007() || [];
                log.debug('vector_ingresos ' + vector_ingresos.length, vector_ingresos);

                var vector_ingresos_journal = ObtieneIngresosJournal1007() || [];
                vector_ingresos = vector_ingresos.concat(vector_ingresos_journal);
                log.debug('vector_ingresos JOURNAL ' + vector_ingresos_journal.length, vector_ingresos_journal);


                var vector_currency = ObtieneIngresosCurrency1007() || [];
                vector_ingresos = vector_ingresos.concat(vector_currency);
                log.debug('vector_currency' + vector_currency.length, vector_currency);

                if (vector_ingresos.length) {
                    return vector_ingresos
                } else {
                    NoData('1');
                }
            } catch (error) {
                log.error('Error de getInputData', error);
                return [{ "isError": "T", "error": error }];
            }

        }

        function ObtenerVendorExp(id_expense) {
            var id_vendor = '';
            var transactionSearchObj = search.create({
                type: "transaction",
                filters: [
                    ["internalidnumber", "equalto", id_expense],
                    "AND", ["formulanumeric: CASE WHEN NVL({custcol_lmry_exp_rep_vendor_colum.internalid},0) = 0 THEN 0 ELSE 1 END", "equalto", "1"]
                ],
                columns: [
                    search.createColumn({
                        name: "internalid",
                        join: "CUSTCOL_LMRY_EXP_REP_VENDOR_COLUM"
                    })
                ]
            });

            var searchresult = transactionSearchObj.run();
            var objResult = searchresult.getRange(0, 1000);
            if (objResult.length != 0) {
                var columns = objResult[0].columns;
                id_vendor = objResult[0].getValue(columns[0]);
                return id_vendor;
            } else {
                return '';
            }

        }


        /**
         * If this entry point is used, the map function is invoked one time for each key/value.
         *
         * @param {Object} context
         * @param {boolean} context.isRestarted - Indicates whether the current invocation represents a restart
         * @param {number} context.executionNo - Version of the bundle being installed
         * @param {Iterator} context.errors - This param contains a "iterator().each(parameters)" function
         * @param {string} context.key - The key to be processed during the current invocation
         * @param {string} context.value - The value to be processed during the current invocation
         * @param {function} context.write - This data is passed to the reduce stage
         *
         * @since 2016.1
         */
        function map(context) {

            try {
                var objResult = JSON.parse(context.value);
                if (objResult["isError"] == "T") {
                    context.write({
                        key: context.key,
                        value: objResult
                    });
                } else {
                    ObtenerPaises();
                    //log.debug('objResult', objResult);
                    var accountDetailJson = getTransactionDetail(objResult);

                    if (Array.isArray(accountDetailJson) == true && accountDetailJson.length != 0) {
                        context.write({
                            key: accountDetailJson[0],
                            value: {
                                stringTransaction: accountDetailJson[1]
                            }
                        });

                        //log.debug('accountDetailJson[1]', accountDetailJson[1]);
                    }
                    // log.debug('accountDetailJson[1]  ' + accountDetailJson[0], accountDetailJson[1]);
                    // for (var key in accountDetailJson) {
                    //     log.debug('ejecutnado',accountDetailJson[key]);
                    //     context.write({
                    //         key: key,
                    //         value: accountDetailJson[key]
                    //     });
                    // }
                }
            } catch (error) {
                log.error('Error de Map', error);
                context.write({
                    key: objResult[0],
                    value: {
                        isError: "T",
                        error: error
                    }
                });
            }

        }


        /**
         * If this entry point is used, the reduce function is invoked one time for
         * each key and list of values provided..
         *
         * @param {Object} context
         * @param {boolean} context.isRestarted - Indicates whether the current invocation represents a restart
         * @param {number} context.executionNo - Version of the bundle being installed
         * @param {Iterator} context.errors - This param contains a "iterator().each(parameters)" function
         * @param {string} context.key - The key to be processed during the current invocation
         * @param {string} context.value - The value to be processed during the current invocation
         * @param {function} context.write - This data is passed to the reduce stage
         *
         * @since 2016.1
         */
        function reduce(context) {
            try {
                var vectorMap = [];
                var row_retenido = [];
                var sumacred = 0;
                var sumadeb = 0;

                vectorMap = context.values;
                // namEmpresa = context.key;
                var idTransac = '1';

                log.debug('REDUCE', vectorMap);

                for (var j = 0; j < vectorMap.length; j++) {

                    var obj = JSON.parse(vectorMap[j]);

                    if (obj["isError"] == "T") {

                        context.write({
                            key: context.key,
                            value: obj
                        });
                        return;
                    } else {
                        //log.debug('REDUCE', obj["stringTransaction"]);
                        row_retenido = obj["stringTransaction"].split('|');
                        sumacred = sumacred + Number(row_retenido[9]);
                        sumadeb = sumadeb + Number(row_retenido[10]);
                    }


                    // log.debug('searchResult resultado'+sumacred+'--'+sumadeb,row_retenido);

                }
                var aux_agrupado = row_retenido[8] + '|' + row_retenido[0] + '|' + row_retenido[1] + '|' + row_retenido[2] + '|' + row_retenido[3] + '|' + row_retenido[4] + '|' + row_retenido[5] + '|' + row_retenido[6] + '|' + row_retenido[7] + '|' + sumacred + '|' + sumadeb;
                var cuantia_menor_linea = sumacred + sumadeb;
                // if (cuantia_menor_linea >= CUANTIA_MINIMA) {

                // }else{
                //     var aux_agrupado2 = row_retenido[8] + ',' + row_retenido[0] + ',' + row_retenido[1] + ',' + row_retenido[2] + ',' + row_retenido[3] + ',' + row_retenido[4] + ',' + row_retenido[5] + ',' + row_retenido[6] + ',' + row_retenido[7] + ',' + sumacred + ',' + sumadeb;

                // }
                if (row_retenido != '') {
                    log.debug('aux_agrupado', aux_agrupado);
                    context.write({
                        key: idTransac,
                        value: {
                            strRetencionesIva: aux_agrupado
                        }
                    });
                }

                // }

            } catch (error) {
                log.error('Error de Reduce', error);
                context.write({
                    key: context.key,
                    value: {
                        isError: "T",
                        error: error
                    }
                });
            }


        }

        /**
         * If this entry point is used, the reduce function is invoked one time for
         * each key and list of values provided..
         *
         * @param {Object} context
         * @param {boolean} context.isRestarted - Indicates whether the current invocation of the represents a restart.
         * @param {number} context.concurrency - The maximum concurrency number when running the map/reduce script.
         * @param {Date} context.datecreated - The time and day when the script began running.
         * @param {number} context.seconds - The total number of seconds that elapsed during the processing of the script.
         * @param {number} context.usage - TThe total number of usage units consumed during the processing of the script.
         * @param {number} context.yields - The total number of yields that occurred during the processing of the script.
         * @param {Object} context.inputSummary - Object that contains data about the input stage.
         * @param {Object} context.mapSummary - Object that contains data about the map stage.
         * @param {Object} context.reduceSummary - Object that contains data about the reduce stage.
         * @param {Iterator} context.ouput - This param contains a "iterator().each(parameters)" function
         *
         * @since 2016.1
         */
        function summarize(context) {
            try {
                ObtenerParametrosYFeatures();
                ObtenerDatosSubsidiaria();
                var i = 0;
                var json_final = {};
                var vectorReduce = [];
                var vectorCuantiasMenores = [];
                var vectorFinal = [];
                var concepto_val_total = 0;
                //Vector para almacenar vectores de retenciones que se obtuvieron de la funcion reduce
                //Tam vector_retenciones = cantidad de lineas del reporte
                var vector_retenciones = [];
                var errores = [];

                context.output.iterator().each(function(key, value) {
                    var obj = JSON.parse(value);
                    if (obj["isError"] == "T") {
                        errores.push(JSON.stringify(obj["error"]));
                    } else {
                        var contenido_retenciones = obj.strRetencionesIva;

                        vectorReduce = obj.strRetencionesIva.split('|');
                        vector_retenciones.push(vectorReduce);
                    }
                    return true;


                });
                for (var i = 0; i < vector_retenciones.length; i++) {

                    var cuantia_menor_linea = Number(vector_retenciones[i][9]) - Number(vector_retenciones[i][10]);
                    if (Number(vector_retenciones[i][9]) >= Number(CUANTIA_MINIMA)) {
                        if (param_detallado == 'F' || param_detallado == false) {
                            var dif_valores = Number(vector_retenciones[i][9]) - Number(vector_retenciones[i][10]);
                            if (Number(dif_valores) < 0) {
                                dif_valores = Number(dif_valores) * (-1);
                                vector_retenciones[i][9] = 0;
                                vector_retenciones[i][10] = Number(dif_valores);
                            } else {
                                vector_retenciones[i][9] = Number(dif_valores);
                                vector_retenciones[i][10] = 0;
                            }
                            vectorFinal.push(vector_retenciones[i]);
                        } else {
                            vectorFinal.push(vector_retenciones[i]);
                        }

                    } else {
                        log.debug('CUANTIAS LINEAS', vector_retenciones[i]);
                        vectorCuantiasMenores.push(vector_retenciones[i]);
                    }
                }

                var vectorCuantias = GenerarIngresosPorCuantias(vectorCuantiasMenores, vectorFinal);
                // log.debug('vectorFinal' + vectorFinal.length, vectorFinal);
                for (var j = 0; j < vectorCuantias.length; j++) {

                    concepto_val_total = concepto_val_total + Number(vectorCuantias[j][0]);
                    // log.debug('CONCEPTOS FINAL', vectorCuantias[j][0] + '-' + concepto_val_total);

                }
                var numeroEnvio = obtenerNumeroEnvio();
                log.debug('errores', errores);
                if (errores.length > 0) {
                    libreria.sendMail(LMRY_script, errores[0]);
                    NoData('2');
                } else {
                    if (vectorCuantias.length != 0) {
                        GenerarXml(vectorCuantias, concepto_val_total, numeroEnvio);
                        GenerarExcel(vectorCuantias, numeroEnvio);
                    } else {
                        NoData('1');
                    }

                }

            } catch (error) {
                libreria.sendMail(LMRY_script, error);
                NoData('2');
            }
        }

        function GenerarXml(vector, valorTotal, numeroEnvio) {

            var xmlString = '';
            var strXmlVentasXPagar = '';
            var cantidadDatos = 0;
            var today = new Date();
            var anio = today.getFullYear();
            var mes = completar_cero(2, today.getMonth() + 1);
            var day = completar_cero(2, today.getDate());
            var hour = completar_cero(2, today.getHours());
            var min = completar_cero(2, today.getMinutes());
            var sec = completar_cero(2, today.getSeconds());
            today = anio + '-' + mes + '-' + day + 'T' + hour + ':' + min + ':' + sec;

            for (var i = 0; i < vector.length; i++) {
                // log.debug('vector xml',vector);
                if (vector[i][9] > 0 || vector[i][10] > 0) {
                    xmlString += '<ingresos dred="' + Number(vector[i][10]).toFixed(0) + '" ibru="' + Number(vector[i][9]).toFixed(0) + '" pais="' + vector[i][8];

                    if (vector[i][7]) {
                        xmlString += '" raz="' + vector[i][7].replace(/&/g, '&amp;');
                    } else {
                        xmlString += '" raz="';
                    }

                    if (vector[i][6]) {
                        xmlString += '" nomb2="' + vector[i][6].replace(/&/g, '&amp;');
                    } else {
                        xmlString += '" nomb2="';
                    }

                    if (vector[i][5]) {
                        xmlString += '" nomb1="' + vector[i][5].replace(/&/g, '&amp;');
                    } else {
                        xmlString += '" nomb1="';
                    }

                    if (vector[i][4]) {
                        xmlString += '" apl2="' + vector[i][4].replace(/&/g, '&amp;');
                    } else {
                        xmlString += '" apl2="';
                    }

                    if (vector[i][3]) {
                        xmlString += '" apl1="' + vector[i][3].replace(/&/g, '&amp;');
                    } else {
                        xmlString += '" apl1="';
                    }

                    xmlString += '" nid="' + vector[i][2] + '" tdoc="' + vector[i][1] + '" cpt="' + vector[i][0];

                    xmlString += '"/> \r\n';
                    cantidadDatos++;
                }

            }
            strXmlVentasXPagar += '<?xml version="1.0" encoding="ISO-8859-1"?> \r\n';
            strXmlVentasXPagar += '<mas xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"> \r\n';
            strXmlVentasXPagar += '<Cab> \r\n';
            strXmlVentasXPagar += '<Ano>' + paramPeriodo + '</Ano> \r\n';
            strXmlVentasXPagar += '<CodCpt>' + paramConcepto + '</CodCpt> \r\n';
            strXmlVentasXPagar += '<Formato>1007</Formato> \r\n';
            strXmlVentasXPagar += '<Version>9</Version> \r\n';
            strXmlVentasXPagar += '<NumEnvio>' + numeroEnvio + '</NumEnvio> \r\n';
            strXmlVentasXPagar += '<FecEnvio>' + today + '</FecEnvio> \r\n';
            strXmlVentasXPagar += '<FecInicial>' + paramPeriodo + '-01-01</FecInicial> \r\n';
            strXmlVentasXPagar += '<FecFinal>' + paramPeriodo + '-12-31</FecFinal> \r\n';
            strXmlVentasXPagar += '<ValorTotal>' + valorTotal + '</ValorTotal> \r\n';
            strXmlVentasXPagar += '<CantReg>' + cantidadDatos + '</CantReg> \r\n';
            strXmlVentasXPagar += '</Cab>\r\n';
            strXmlVentasXPagar += xmlString;
            strXmlVentasXPagar += '</mas> \r\n';

            //log.error("strXmlVentasXPagar", strXmlVentasXPagar);

            SaveFile('.xml', strXmlVentasXPagar, numeroEnvio);
        }

        function completar_cero(long, valor) {

            if ((('' + valor).length) <= long) {
                if (long != ('' + valor).length) {
                    for (var i = (('' + valor).length); i < long; i++) {
                        valor = '0' + valor;
                    }
                } else {
                    return valor;
                }
                return valor;
            } else {
                valor = valor.substring(0, long);
                return valor;
            }

        }

        function SaveFile(extension, strArchivo, numeroEnvio) {
            try {
                var folderId = objContext.getParameter({
                    name: 'custscript_lmry_file_cabinet_rg_co'
                });

                var generarXml = false;

                if (hasSubsidiariaFeature) {
                    var subsidiaryRecord = search.lookupFields({
                        type: search.Type.SUBSIDIARY,
                        id: paramSubsidiaria,
                        columns: ['legalname']
                    });
                    var companyName = subsidiaryRecord.legalname;
                } else {
                    var pageConfig = config.load({
                        type: config.Type.COMPANY_INFORMATION
                    });
                    var companyName = pageConfig.getValue('legalname');
                }

                // Almacena en la carpeta de Archivos Generados
                if (folderId != '' && folderId != null) {
                    // Extension del archivo

                    var fileName = Name_File(numeroEnvio) + extension;

                    log.error("fileName", fileName);

                    // Crea el archivo
                    var ventasXPagarFile;

                    if (extension == '.xls') {
                        //log.error("strExcelVentasXPagar", strArchivo);
                        ventasXPagarFile = file.create({
                            name: fileName,
                            fileType: file.Type.EXCEL,
                            contents: strArchivo,
                            folder: folderId
                        });

                    } else {
                        //log.error("strXmlVentasXPagar", strArchivo);
                        ventasXPagarFile = file.create({
                            name: fileName,
                            fileType: file.Type.XMLDOC,
                            contents: strArchivo,
                            folder: folderId
                        });

                        generarXml = true;
                    }

                    var fileId = ventasXPagarFile.save();

                    ventasXPagarFile = file.load({
                        id: fileId
                    });

                    var getURL = objContext.getParameter({
                        name: 'custscript_lmry_netsuite_location'
                    });

                    var fileUrl = '';

                    if (getURL != '') {
                        fileUrl += 'https://' + getURL;
                    }

                    fileUrl += ventasXPagarFile.url;
                    log.error('fileUrl', fileUrl);

                    if (fileId) {

                        if (paramIdReport) {
                            var report = search.lookupFields({
                                type: 'customrecord_lmry_co_features',
                                id: paramIdReport,
                                columns: ['name']
                            });
                            reportName = report.name;
                        }

                        var usuario = runtime.getCurrentUser();
                        var employee = search.lookupFields({
                            type: search.Type.EMPLOYEE,
                            id: usuario.id,
                            columns: ['firstname', 'lastname']
                        });
                        var usuarioName = employee.firstname + ' ' + employee.lastname;
                        log.debug('paramIdLog' + generarXml, usuarioName + '-' + paramIdLog);
                        if (generarXml) {
                            var recordLog = record.create({
                                type: 'customrecord_lmry_co_rpt_generator_log'
                            });
                        } else {
                            var recordLog = record.load({
                                type: 'customrecord_lmry_co_rpt_generator_log',
                                id: paramIdLog
                            });
                        }
                        //Nombre de Archivo
                        recordLog.setValue({
                            fieldId: 'custrecord_lmry_co_rg_name',
                            value: fileName
                        });

                        //Url de Archivo
                        recordLog.setValue({
                            fieldId: 'custrecord_lmry_co_rg_url_file',
                            value: fileUrl
                        });

                        //Nombre de Reporte
                        recordLog.setValue({
                            fieldId: 'custrecord_lmry_co_rg_transaction',
                            value: reportName
                        });

                        //Nombre de Subsidiaria
                        recordLog.setValue({
                            fieldId: 'custrecord_lmry_co_rg_subsidiary',
                            value: companyName
                        });

                        //Periodo
                        recordLog.setValue({
                            fieldId: 'custrecord_lmry_co_rg_postingperiod',
                            value: paramPeriodo
                        });

                        if (hasMultibookFeature) {
                            //Multibook

                            var multibookName = search.lookupFields({
                                type: search.Type.ACCOUNTING_BOOK,
                                id: paramMultibook,
                                columns: ['name']
                            }).name;

                            recordLog.setValue({
                                fieldId: 'custrecord_lmry_co_rg_multibook',
                                value: multibookName
                            });
                        }

                        //Creado Por
                        recordLog.setValue({
                            fieldId: 'custrecord_lmry_co_rg_employee',
                            value: usuarioName
                        });

                        recordLog.save();
                        libreria.sendrptuser(reportName, 3, fileName);
                    }
                } else {
                    log.error({
                        title: 'Creacion de File:',
                        details: 'No existe el folder'
                    });

                }
            } catch (error) {
                log.error('ERROR SAVE', error);
            }


        }

        function Name_File(numeroEnvio) {
            var name = '';
            name = "Dmuisca_" + completar_cero(2, paramConcepto) + '01007' + '09' + paramPeriodo + completar_cero(8, numeroEnvio);
            return name;
        }

        function GenerarIngresosPorCuantias(cuantiasMenoresArray, ingresosRecibidosArray) {
            try {
                var cuantiasAgrupadasArray = [];
                var recorridos = {};
                var cuenta, sumaIngBrutos = 0,
                    sumaDevol = 0;
                var arrayLength = ingresosRecibidosArray.length;
                var buscarHasta = arrayLength;

                for (var i = 0; i < cuantiasMenoresArray.length; i++) {
                    log.debug('cuantiasMenoresArray', cuantiasMenoresArray[i]);
                    if (cuantiasMenoresArray[i][0] && !recorridos[cuantiasMenoresArray[i][0]]) {
                        cuantiasAgrupadasArray = [];
                        sumaIngBrutos = 0;
                        sumaDevol = 0;
                        recorridos[cuantiasMenoresArray[i][0]] = 1;

                        for (var j = 0; j < cuantiasMenoresArray.length; j++) {
                            if (cuantiasMenoresArray[i][0] == cuantiasMenoresArray[j][0]) {
                                log.debug('cuantiasMenoresArray dentro if', cuantiasMenoresArray[j]);

                                sumaIngBrutos = sumaIngBrutos + Number(cuantiasMenoresArray[j][9]);
                                sumaDevol = sumaDevol + Number(cuantiasMenoresArray[j][10]);
                            }
                        }

                        var id = BuscarIngresoRecibido(cuantiasMenoresArray[i][0], buscarHasta, ingresosRecibidosArray);
                        if (id != -1) {
                            log.debug('ingresosRecibidosArray[id] antes', ingresosRecibidosArray[id]);
                            ingresosRecibidosArray[id][9] = Number(ingresosRecibidosArray[id][9]) + sumaIngBrutos;
                            ingresosRecibidosArray[id][10] = Number(ingresosRecibidosArray[id][10]) + sumaDevol;

                            var diferncia = Number(ingresosRecibidosArray[id][9]) - Number(ingresosRecibidosArray[id][10]);
                            if (Number(diferncia) < 0) {
                                diferncia = Number(diferncia) * (-1);
                                ingresosRecibidosArray[id][9] = 0;
                                ingresosRecibidosArray[id][10] = diferncia;
                            } else {
                                ingresosRecibidosArray[id][9] = diferncia;
                                ingresosRecibidosArray[id][10] = 0;
                            }
                            log.debug('ingresosRecibidosArray[id]', ingresosRecibidosArray[id]);

                        } else {
                            cuantiasAgrupadasArray[0] = cuantiasMenoresArray[i][0];
                            cuantiasAgrupadasArray[1] = '43';
                            cuantiasAgrupadasArray[2] = '222222222';
                            cuantiasAgrupadasArray[3] = '';
                            cuantiasAgrupadasArray[4] = '';
                            cuantiasAgrupadasArray[5] = '';
                            cuantiasAgrupadasArray[6] = '';
                            cuantiasAgrupadasArray[7] = 'CUANTIAS MENORES';
                            cuantiasAgrupadasArray[8] = '169';
                            cuantiasAgrupadasArray[9] = sumaIngBrutos;
                            cuantiasAgrupadasArray[10] = sumaDevol;
                            var diferncia = Number(sumaIngBrutos) - Number(sumaDevol);
                            if (Number(diferncia) < 0) {
                                diferncia = Number(diferncia) * (-1);
                                cuantiasAgrupadasArray[9] = 0;
                                cuantiasAgrupadasArray[10] = diferncia;
                            } else {
                                cuantiasAgrupadasArray[9] = diferncia;
                                cuantiasAgrupadasArray[10] = 0;
                            }

                            log.debug('cuantiasAgrupadasArray', cuantiasAgrupadasArray);
                            ingresosRecibidosArray.push(cuantiasAgrupadasArray);
                            arrayLength++;
                            // if (arrayLength <= MAX_REGISTROS_REPORTE) {
                            //     valorTotal = valorTotal + Number(cuantiasMenoresArray[i][0]);
                            // }
                        }


                    }
                }

                return ingresosRecibidosArray;
            } catch (e) {
                log.error('ERROR CUANTIAS MENORES', e);
            }

        }

        function obtenerNumeroEnvio() {

            var numeroLote = 1;
            log.error('idfeatyre', paramIdFeatureByVersion);
            log.error('idsubsi', paramSubsidiaria);


            var savedSearch = search.create({
                type: 'customrecord_lmry_co_lote_rpt_magnetic',
                filters: [
                    search.createFilter({
                        name: 'internalid',
                        join: 'custrecord_lmry_co_id_magnetic_rpt',
                        operator: search.Operator.IS,
                        values: [paramIdFeatureByVersion]
                    }),
                    search.createFilter({
                        name: 'internalid',
                        join: 'custrecord_lmry_co_subsidiary',
                        operator: search.Operator.IS,
                        values: [paramSubsidiaria]
                    })
                ],
                columns: ['internalid', 'custrecord_lmry_co_lote']
            });
            var objResult = savedSearch.run().getRange(0, 1000);

            if (objResult == null || objResult.length == 0) {

                var loteXRptMgnRecord = record.create({
                    type: 'customrecord_lmry_co_lote_rpt_magnetic'
                });

                loteXRptMgnRecord.setValue({
                    fieldId: 'custrecord_lmry_co_id_magnetic_rpt',
                    value: paramIdFeatureByVersion
                });

                loteXRptMgnRecord.setValue({
                    fieldId: 'custrecord_lmry_co_year_issue',
                    value: paramPeriodo
                });

                loteXRptMgnRecord.setValue({
                    fieldId: 'custrecord_lmry_co_lote',
                    value: numeroLote
                });

                loteXRptMgnRecord.setValue({
                    fieldId: 'custrecord_lmry_co_subsidiary',
                    value: paramSubsidiaria
                })

                loteXRptMgnRecord.save();

            } else {
                var columns = objResult[0].columns;
                var internalId = objResult[0].getValue(columns[0]);
                numeroLote = Number(objResult[0].getValue(columns[1])) + 1;
                var loteXRptMgnRecord = record.load({
                    type: 'customrecord_lmry_co_lote_rpt_magnetic',
                    id: internalId
                });

                loteXRptMgnRecord.setValue({
                    fieldId: 'custrecord_lmry_co_lote',
                    value: numeroLote
                });

                loteXRptMgnRecord.save();
            }

            return numeroLote;
        }

        function BuscarIngresoRecibido(concepto, buscarHasta, ingresosRecibidosArray) {
            var id = 0;

            for (var i = 0; i < buscarHasta; i++) {
                if (concepto == ingresosRecibidosArray[i][0] && ingresosRecibidosArray[i][1] == '43' && ingresosRecibidosArray[i][2] == '222222222' && ingresosRecibidosArray[i][7] == 'CUANTIAS MENORES' && ingresosRecibidosArray[i][8] == '169') {
                    return i;
                }
            }
            return -1;
        }

        function ObtenerParametrosYFeatures() {

            paramSubsidiaria = objContext.getParameter({
                name: "custscript_lmry_subs_form1007anualv9"
            });

            paramPeriodo = objContext.getParameter({
                name: "custscript_lmry_periodo_form1007anualv9"
            });

            paramMultibook = objContext.getParameter({
                name: "custscript_lmry_multi_form1007anualv9"
            });

            paramIdReport = objContext.getParameter({
                name: "custscript_lmry_feature_form1007anualv9"
            });

            paramIdLog = objContext.getParameter({
                name: "custscript_lmry_idlog_form1007anualv9"
            });

            paramIdFeatureByVersion = objContext.getParameter({
                name: "custscript_lmry_idfbv_form1007anualv9"
            });

            paramConcepto = objContext.getParameter({
                name: "custscript_lmry_concept_form1007anualv9"
            });


            param_detallado = objContext.getParameter({
                name: "custscript_lmry_detalla_form1007anualv9"
            });

            //Features
            hasSubsidiariaFeature = runtime.isFeatureInEffect({
                feature: "SUBSIDIARIES"
            });

            log.debug('subsidiaria', hasSubsidiariaFeature + '-' + paramSubsidiaria);

            hasMultibookFeature = runtime.isFeatureInEffect({
                feature: "MULTIBOOK"
            });

            hasJobFeature = runtime.isFeatureInEffect({
                feature: "JOBS"
            });

            isAdvanceJobsFeature = runtime.isFeatureInEffect({
                feature: "ADVANCEDJOBS"
            });

            paramCont = paramCont || "0";

            log.debug('PARAMETROS', {
                paramSubsidiaria: paramSubsidiaria,
                paramPeriodo: paramPeriodo,
                paramMultibook: paramMultibook,
                paramIdReport: paramIdReport,
                paramIdFeatureByVersion: paramIdFeatureByVersion,
                paramConcepto: paramConcepto,
                param_detallado: param_detallado
            });

            if (paramIdReport) {
                var recordReport = search.lookupFields({
                    type: "customrecord_lmry_co_features",
                    id: paramIdReport,
                    columns: ["name"]
                });
                reportName = recordReport.name;
            }

            if (hasMultibookFeature) {
                var recordMultibook = search.lookupFields({
                    type: search.Type.ACCOUNTING_BOOK,
                    id: paramMultibook,
                    columns: ["name"]
                });
                multibookName = recordMultibook.name;
            }

            if (paramIdFeatureByVersion) {
                var recordFeatureByVersion = search.lookupFields({
                    type: "customrecord_lmry_co_rpt_feature_version",
                    id: paramIdFeatureByVersion,
                    columns: ["custrecord_lmry_co_amount"]
                });
                CUANTIA_MINIMA = recordFeatureByVersion.custrecord_lmry_co_amount;
            }

            log.debug('paramIdFeatureByVersion', paramIdFeatureByVersion);
            log.debug('CUANTIA_MINIMA byversion', CUANTIA_MINIMA);
            log.debug('PARAM DETALLADO', param_detallado);

            periodStartDate = new Date(paramPeriodo, 0, 1);
            mes_date = periodStartDate.getMonth() + 1;
            anio_date = periodStartDate.getFullYear();
            var DD = periodStartDate.getDate();

            periodStartDate = format.format({
                value: periodStartDate,
                type: format.Type.DATE
            });
            formato_stardate = completar_cero(2, DD) + '/' + completar_cero(2, mes_date) + '/' + anio_date;

            periodEndDate = new Date(paramPeriodo, 11, 31);
            var mes_date_fin = periodEndDate.getMonth() + 1;
            var anio_date_fin = periodEndDate.getFullYear();
            var DD_fin = periodEndDate.getDate();

            periodEndDate = format.format({
                value: periodEndDate,
                type: format.Type.DATE
            });
            formato_enddate = DD_fin + '/' + mes_date_fin + '/' + anio_date_fin;

            log.debug('formato_stardate', formato_stardate + '-' + formato_enddate);
            periodName = paramPeriodo;
        }

        function ObtenerCurrencies() {
            var savedSearch = search.create({
                type: "currency",
                filters: [],
                columns: [
                    "internalid",
                    "name",
                    "symbol"
                ]
            });

            var objResult = savedSearch.run().getRange(0, 1000);

            if (objResult != null && objResult.length != 0) {
                var auxArray, columns;
                for (var i = 0; i < objResult.length; i++) {
                    columns = objResult[i].columns;

                    auxArray = [];
                    auxArray[0] = objResult[i].getValue(columns[0]);
                    auxArray[1] = objResult[i].getValue(columns[1]);
                    auxArray[2] = objResult[i].getValue(columns[2]);

                    currenciesJson[auxArray[0]] = auxArray;
                }
            }
        }

        function getTransactionDetail(objResult) {
            try {
                var resultArray = [];
                var key = '';
                var valuemap = '';
                var accountsDetailJson = {};
                var vectoraux = [];
                var transactionType = objResult[0];
                var vendorId = objResult[3];
                var customerId = objResult[4];
                var employeeId = objResult[5];


                var entidad = '',
                    internalId = '';

                if (transactionType == 'FxReval') {
                    var internalIdCurrency = objResult[7];
                    var datos_entidad = ObtenerIdEntidad(internalIdCurrency).split('|');
                    // log.debug('ENTRO A UN CURRENCY',datos_entidad);
                    entidad = datos_entidad[0];
                    internalId = datos_entidad[1];
                }

                if (transactionType == 'VendBill' || transactionType == 'VendCred' || transactionType == 'VendPymt' || transactionType == 'CardChrg' || transactionType == 'ExpRept' && vendorId != '') {
                    entidad = 'vendor';
                    internalId = vendorId;
                } else if (transactionType == 'CustInvc' || transactionType == 'CustCred' || transactionType == 'CustPymt' || transactionType == 'CashSale' && customerId != '') {
                    entidad = 'customer';
                    internalId = customerId;
                } else if (transactionType == 'Journal' || transactionType == 'Check') {
                    if (vendorId != '') {
                        entidad = 'vendor';
                        internalId = vendorId;
                    } else {
                        if (customerId != '') {
                            entidad = 'customer';
                            internalId = customerId;
                        } else {
                            entidad = 'employee';
                            internalId = employeeId;
                        }
                    }
                }

                // TRAER INFORMACION DE LOS 7 CAMPOS
                // if (transactionType == 'ExpRept') {
                //     log.error("resultArray function " + transactionType, entidad + '--' + internalId);

                // }
                var aux_array = [];
                if (internalId != '' || internalId != 0) {
                    if (transactionType == 'Journal' || transactionType == 'Check') {
                        var aux_default = getDefaultBillingJournal(internalId);
                        if ((aux_default == 'true' || aux_default == true) && employeeId != '' && entidad == 'employee') {
                            aux_array = getInformationJournal(internalId);
                            resultArray = resultArray.concat(aux_array);
                        } else {
                            if ((aux_default == 'true' || aux_default == true)) {
                                aux_array = getInformation(entidad, internalId);
                                resultArray = resultArray.concat(aux_array);
                            } else {
                                log.debug('NO TIENE BILLING', aux_default);
                            }
                        }
                    } else {
                        var aux_default = getDefaultBillingJournal(internalId);
                        if ((aux_default == 'true' || aux_default == true)) {
                            aux_array = getInformation(entidad, internalId);
                            resultArray = resultArray.concat(aux_array);
                        } else {
                            log.debug('NO TIENE BILLING', aux_default);
                        }
                    }
                } else {
                    aux_array = ['43', '222222222', '', '', '', '', 'CUANTIAS MENORES', '169'];
                    resultArray = resultArray.concat(aux_array);
                }

                //log.debug('Result', aux_array);
                if (resultArray.length != 0) {

                    // resultArray[8] = Number(objResult[3]);
                    // log.debug('conceptos id', objResult[5]);
                    var id_cuenta = Number(objResult[6]);
                    var concepto = ObtenerConceptoAccount(id_cuenta);
                    // var id_trans = search.lookupFields({
                    //     type: search.Type.ACCOUNT,
                    //     id: id_cuenta,
                    //     columns: ['custrecord_lmry_co_puc_concept.custrecord_lmry_co_puc_number_c']
                    // });

                    // var var3 = id_trans['custrecord_lmry_co_puc_concept.custrecord_lmry_co_puc_number_c'];

                    resultArray[8] = Number(concepto);
                    resultArray[9] = Number(objResult[1]);
                    resultArray[10] = Number(objResult[2]);

                    key = resultArray[0] + "|" + resultArray[1] + "|" + resultArray[2] + "|" + resultArray[3] + "|" + resultArray[4] + "|" + resultArray[5] + "|" + resultArray[6] + "|" + resultArray[7] + "|" + resultArray[8];
                    valuemap = resultArray[0] + "|" + resultArray[1] + "|" + resultArray[2] + "|" + resultArray[3] + "|" + resultArray[4] + "|" + resultArray[5] + "|" + resultArray[6] + "|" + resultArray[7] + "|" + resultArray[8] + "|" + resultArray[9] + "|" + resultArray[10];

                    vectoraux[0] = key;
                    vectoraux[1] = valuemap;

                    accountsDetailJson[key] = resultArray;

                    return vectoraux;
                }
            } catch (e) {
                log.error('ERROR EN ENTIDAD', e);
            }

        }

        function ObtenerConceptoAccount(idAccount) {
            try {
                var concepto_enviar = '';
                var accountSearchObj = search.create({
                    type: "account",
                    filters: [
                        ["internalidnumber", "equalto", idAccount],
                        "AND", ["custrecord_lmry_co_puc_concept.custrecord_lmry_co_puc_formatid_c", "is", "1007"]
                    ],
                    columns: [
                        search.createColumn({
                            name: "custrecord_lmry_co_puc_number_c",
                            join: "CUSTRECORD_LMRY_CO_PUC_CONCEPT"
                        })
                    ]
                });
                var objResult = accountSearchObj.run().getRange(0, 1);
                if (objResult && objResult.length) {
                    var columns = objResult[0].columns;

                    // 1. Concepto
                    if (objResult[0].getValue(columns[0])) {
                        concepto_enviar = objResult[0].getValue(columns[0]);
                    } else {
                        concepto_enviar = '';
                    }
                }

                return concepto_enviar;
            } catch (e) {
                log.error('[ERROR EN OBTENER CONCEPTO]', e);
            }


        }

        function ObtenerIdEntidad(idCurrency) {
            var auxArray = [];

            if (idCurrency == "46892") {
                log.error("idCurrency", idCurrency);
            }

            var fxrevalSearchObj = search.create({
                type: "fxreval",
                filters: [
                    ["type", "anyof", "FxReval"],
                    "AND", ["internalidnumber", "equalto", idCurrency],
                    "AND", ["formulatext: CASE WHEN NVL({name},'NULL')='NULL' THEN 1 ELSE 0 END", "is", "0"]
                ],
                columns: [
                    search.createColumn({
                        name: "formulatext",
                        formula: "{vendorline.internalid}"
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "{customer.internalid}"
                    })
                ]
            });

            var objResult = fxrevalSearchObj.run().getRange(0, 100);
            if (objResult && objResult.length) {
                var columns = objResult[0].columns;

                // 2. Apellido Paterno
                if (objResult[0].getValue(columns[0])) {
                    auxArray[0] = objResult[0].getValue(columns[0]);
                } else {
                    auxArray[0] = '';
                }

                // 2. Apellido Paterno
                if (objResult[0].getValue(columns[1])) {
                    auxArray[1] = objResult[0].getValue(columns[1]);
                } else {
                    auxArray[1] = '';
                }
                if (auxArray[0] != '') {
                    var entidad = 'vendor';
                    var id_currency_entidad = auxArray[0];
                } else {
                    var entidad = 'customer';
                    var id_currency_entidad = auxArray[1];
                }


            } else {
                log.error("[ERROR EN ObtenerIdEntidad]", 'no tiene entidad');
            }
            // log.debug('datos customer', auxArray);
            if (idCurrency == "46892") {
                log.error("idCurrency 46892", entidad + '|' + id_currency_entidad);
            }
            return entidad + '|' + id_currency_entidad;

        }

        function getInformationJournal(internalId) {
            var auxArray = [];

            var employeename = search.lookupFields({
                type: search.Type.EMPLOYEE,
                id: internalId,
                columns: ['custentity_lmry_sunat_tipo_doc_cod', 'custentity_lmry_country', 'custentity_lmry_sv_taxpayer_number', 'firstname', 'lastname']
            });
            var tipo_doc_employee = employeename.custentity_lmry_sunat_tipo_doc_cod;
            var pais_employee = employeename.custentity_lmry_country;
            if (pais_employee != '') {
                pais_employee = pais_employee[0].text;
            } else {
                pais_employee = '';
            }

            var firstname = employeename.firstname;
            var lastname = employeename.lastname;
            //log.debug('firstname', firstname + '-' + lastname);
            if (pais_employee != '') {
                if (paisesArray.length > 0) {
                    for (var j = 0; j < paisesArray.length; j++) {
                        if (paisesArray[j][0] == pais_employee) {
                            pais_employee = paisesArray[j][2];
                        }
                    }
                } else {
                    pais_employee = "";
                }
            } else {
                pais_employee = "";
            }


            var vatregnumber_employee = employeename.custentity_lmry_sv_taxpayer_number;
            // var name_employee = employeename.name;

            auxArray[0] = tipo_doc_employee;
            auxArray[1] = QuitarCaracteres(vatregnumber_employee);

            if (firstname.split(' ').length > 1) {
                auxArray[2] = firstname.split(' ')[0];
                auxArray[3] = firstname.split(' ')[1];
            } else {
                auxArray[2] = firstname.split(' ')[0];
                auxArray[3] = '';
            }
            auxArray[2] = auxArray[2].replace(/[|]/gi, '');
            auxArray[3] = auxArray[3].replace(/[|]/gi, '');

            if (lastname.split(' ').length > 1) {
                auxArray[4] = lastname.split(' ')[0];
                auxArray[5] = lastname.split(' ')[1];
            } else {
                auxArray[4] = lastname.split(' ')[0];
                auxArray[5] = '';
            }
            auxArray[4] = auxArray[4].replace(/[|]/gi, '');
            auxArray[5] = auxArray[5].replace(/[|]/gi, '');

            auxArray[6] = '';
            auxArray[7] = pais_employee;

            //log.debug('EMPLOYEE ', auxArray);
            return auxArray;

        }

        function getDefaultBillingJournal(internalId) {
            var auxArray = [];
            var usuario = false;

            if (internalId != '') {

                var entitySearchObj = search.create({
                    type: "entity",
                    filters: [
                        ["internalidnumber", "equalto", internalId],
                        "AND", ["isdefaultbilling", "is", "T"]
                    ],
                    columns: [
                        "isdefaultbilling"
                    ]
                });

                var searchresult = entitySearchObj.run();
                var objResult = searchresult.getRange(0, 100);
                log.debug('objResult journal', objResult);

                if (objResult.length != 0) {
                    var columns = objResult[0].columns;
                    usuario = objResult[0].getValue(columns[0]);

                }
            }
            return usuario;
        }

        function getInformation(entidad, internalId) {

            var auxArray = [];

            if (entidad != '' && internalId != '') {
                var newSearch = search.create({
                    type: entidad,
                    filters: [
                        ['internalid', 'is', internalId]
                    ],
                    columns: [
                        search.createColumn({
                            name: "formulatext",
                            formula: "{custentity_lmry_sunat_tipo_doc_cod}",
                            label: "0. Tipo de Documento"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "{vatregnumber}",
                            label: "1. NIT"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "CASE WHEN {isperson} = 'T' THEN {lastname} ELSE '' END",
                            label: "2. Apellidos"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "CASE WHEN {isperson} = 'T' THEN {firstname} ELSE '' END",
                            label: "3. Nombres"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "CASE WHEN {isperson} = 'F' THEN {companyname} ELSE '' END",
                            label: "4. Razón Social"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "{custentity_lmry_country}",
                            label: "5. PAIS"
                        }),
                    ]
                });

                var objResult = newSearch.run().getRange(0, 1000);
                if (objResult && objResult.length) {
                    var columns = objResult[0].columns;
                    //log.debug('objResult entidad', objResult);
                    // 0. Tipo de Documento
                    auxArray[0] = objResult[0].getValue(columns[0]);
                    if (auxArray[0] == '') {
                        auxArray[0] = '0';
                    }

                    // 1. NIT
                    auxArray[1] = QuitarCaracteres(objResult[0].getValue(columns[1]));

                    // 2. Apellido Paterno
                    if (objResult[0].getValue(columns[2]).split(' ')[0]) {
                        auxArray[2] = objResult[0].getValue(columns[2]).split(' ')[0];
                    } else {
                        auxArray[2] = '';
                    }
                    auxArray[2] = auxArray[2].replace(/[|]/gi, '');

                    // 3. Apellido Materno
                    if (objResult[0].getValue(columns[2]).split(' ')[1]) {
                        auxArray[3] = objResult[0].getValue(columns[2]).split(' ')[1];
                    } else {
                        auxArray[3] = '';
                    }
                    auxArray[3] = auxArray[3].replace(/[|]/gi, '');

                    // 4. Primer Nombre
                    if (objResult[0].getValue(columns[3]).split(' ')[0]) {
                        auxArray[4] = objResult[0].getValue(columns[3]).split(' ')[0];
                    } else {
                        auxArray[4] = '';
                    }
                    auxArray[4] = auxArray[4].replace(/[|]/gi, '');

                    // 5. Segundo Nombre
                    if (objResult[0].getValue(columns[3]).split(' ')[1]) {
                        auxArray[5] = objResult[0].getValue(columns[3]).split(' ')[1];
                    } else {
                        auxArray[5] = '';
                    }
                    auxArray[5] = auxArray[5].replace(/[|]/gi, '');

                    // 6. Razón Social
                    auxArray[6] = objResult[0].getValue(columns[4]);
                    auxArray[6] = auxArray[6].replace(/[|]/gi, '');

                    // 7. Pais
                    auxArray[7] = objResult[0].getValue(columns[5]);
                    if (auxArray[7] != '') {
                        if (paisesArray.length > 0) {
                            for (var j = 0; j < paisesArray.length; j++) {
                                if (paisesArray[j][0] == auxArray[7]) {
                                    auxArray[7] = paisesArray[j][2];
                                }
                            }
                        } else {
                            auxArray[7] = "";
                        }
                    } else {
                        auxArray[7] = "";
                    }

                } else {
                    log.error("entidad no entra 2", entidad);
                    log.error("internalId no entra 2", internalId);
                }
                //log.debug('datos customer', auxArray);
                return auxArray;
            } else {
                log.error("entidad no entra 1", entidad);
                log.error("internalId no entra 1", internalId);
                return [];
            }

        }

        function ObtenerPaises() {
            var intDMaxReg = 1000;
            var intDMinReg = 0;
            var arrAuxiliar = [];
            var DbolStop = false;

            // Consulta de Cuentas
            var savedSearch = search.load({
                id: "customsearch_lmry_co_country"
            })

            var searchResult = savedSearch.run();
            var columns;
            while (!DbolStop) {
                var objResult = searchResult.getRange(intDMinReg, intDMaxReg);

                if (objResult != null) {
                    var intLength = objResult.length;

                    for (var i = 0; i < intLength; i++) {

                        columns = objResult[i].columns;
                        arrAuxiliar = [];

                        //0. NAME
                        if (objResult[i].getValue(columns[0]) != null)
                            arrAuxiliar[0] = objResult[i].getValue(columns[0]);
                        else
                            arrAuxiliar[0] = "";

                        //1. PAIS
                        if (objResult[i].getValue(columns[1]) != null)
                            arrAuxiliar[1] = objResult[i].getText(columns[1]);
                        else
                            arrAuxiliar[1] = "";

                        //2. CODE
                        if (objResult[i].getValue(columns[2]) != null)
                            arrAuxiliar[2] = objResult[i].getValue(columns[2]);
                        else
                            arrAuxiliar[2] = "";

                        //3. NACIONALIDAD
                        if (objResult[i].getValue(columns[3]) != null)
                            arrAuxiliar[3] = objResult[i].getValue(columns[3]);
                        else
                            arrAuxiliar[3] = "";

                        //4. PAIS LOCALIZACION
                        if (objResult[i].getValue(columns[4]) != null)
                            arrAuxiliar[4] = objResult[i].getValue(columns[4]);
                        else
                            arrAuxiliar[4] = "";

                        paisesArray.push(arrAuxiliar);
                    }
                    intDMinReg = intDMaxReg;
                    intDMaxReg += CANT_REGISTROS;
                    if (intLength < CANT_REGISTROS) {
                        DbolStop = true;
                    }
                } else {
                    DbolStop = true;
                }
            }

        }


        function ObtenerDatosSubsidiaria() {
            var configpage = config.load({
                type: config.Type.COMPANY_INFORMATION
            });

            if (hasSubsidiariaFeature) {
                companyName = ObtainNameSubsidiaria(paramSubsidiaria);
                companyRuc = ObtainFederalIdSubsidiaria(paramSubsidiaria);
            } else {
                companyRuc = configpage.getField("employerid");
                companyName = configpage.getField("legalname");
            }

            companyRuc = companyRuc.replace(" ", "");
        }

        function ObtainNameSubsidiaria(subsidiary) {
            try {
                if (subsidiary != "" && subsidiary != null) {
                    var subsidyName = search.lookupFields({
                        type: search.Type.SUBSIDIARY,
                        id: subsidiary,
                        columns: ["legalname"]
                    });


                    return subsidyName.legalname
                }
            } catch (err) {
                libreria.sendMail(LMRY_script, " [ ObtainNameSubsidiaria ] " + err);
            }
            return "";
        }

        function ObtainFederalIdSubsidiaria(subsidiary) {
            try {
                if (subsidiary != "" && subsidiary != null) {
                    var federalId = search.lookupFields({
                        type: search.Type.SUBSIDIARY,
                        id: subsidiary,
                        columns: ["taxidnum"]
                    });

                    return federalId.taxidnum
                }
            } catch (err) {
                libreria.sendMail(LMRY_script, " [ ObtainFederalIdSubsidiaria ] " + err);
            }
            return "";
        }

        function RetornaNumero(nid) {
            if (nid != null && nid != '') {
                return nid.replace(/(\.|\-)/g, '');
            }
            return '';
        }

        function QuitarCaracteres(str) {

            var nit = '';
            for (var i = 0; i < str.length; i++) {
                if (isInteger(Number(str[i])) && str[i] != ' ') {
                    nit += str[i];
                }
            }
            return nit;
        }

        function isInteger(numero) {
            if (numero % 1 == 0) {
                return true;
            } else {
                return false;
            }
        }

        function ObtieneIngresos1007() {
            try {
                var intDMinReg = 0;
                var intDMaxReg = 1000;

                var arrAuxiliar = [];
                var DbolStop = false;
                var contarRegistros = 0;

                // Consulta de Cuentas
                var savedSearch = search.load({
                    id: "customsearch_lmry_co_formmag1007_v9"
                });

                var accountsIdArray = ObtenerCuentas();
                log.error("accountsIdArray", accountsIdArray);

                // var cuentasFormtMM = ObtenerCuentasFormatoMM();

                // log.error("cuentasFormtMM", cuentasFormtMM);

                if (hasSubsidiariaFeature) {
                    var subsidiaryFilter = search.createFilter({
                        name: "subsidiary",
                        operator: search.Operator.IS,
                        values: [paramSubsidiaria]
                    });
                    savedSearch.filters.push(subsidiaryFilter);
                }

                if (paramPeriodo) {
                    var periodStartDateFilter = search.createFilter({
                        name: "trandate",
                        operator: search.Operator.ONORAFTER,
                        values: [periodStartDate]
                    });
                    savedSearch.filters.push(periodStartDateFilter);

                    var periodEndDateFilter = search.createFilter({
                        name: "trandate",
                        operator: search.Operator.ONORBEFORE,
                        values: [periodEndDate]
                    });
                    savedSearch.filters.push(periodEndDateFilter);
                }

                var vendorColumn = search.createColumn({
                    name: 'formulanumeric',
                    formula: "CASE WHEN CONCAT ({Type.id},'') = 'ExpRept' THEN {custcol_lmry_exp_rep_vendor_colum.internalid} ELSE NVL({vendor.internalid},{vendorline.internalid}) END"
                });
                savedSearch.columns.push(vendorColumn);


                if ((hasJobFeature && !isAdvanceJobsFeature)) {
                    log.debug('entra con jobs', 'si jobs');
                    var cuitjobs = search.createColumn({
                        name: "formulanumeric",
                        formula: "CASE WHEN NVL({job.internalid},-1) = -1 THEN {customer.internalid} ELSE {job.customer.id} end"
                    });
                    savedSearch.columns.push(cuitjobs);

                } else {
                    log.debug('entra NO jobs', 'NO jobs');

                    var cuitjobs = search.createColumn({
                        name: 'formulanumeric',
                        formula: '{customer.internalid}'
                    });
                    savedSearch.columns.push(cuitjobs);
                }

                var employeeColumn = search.createColumn({
                    name: 'formulanumeric',
                    formula: "CASE WHEN CONCAT ({Type.id},'') = 'Journal' or CONCAT ({Type.id},'') = 'Check' THEN {entity.id} ELSE 0 END"
                });
                savedSearch.columns.push(employeeColumn);

                var taxCodeColumn = search.createColumn({
                    name: 'formulatext',
                    formula: "CASE WHEN CONCAT ({Type.id},'') = 'Journal' THEN {taxcode} ELSE '' END"
                });
                savedSearch.columns.push(taxCodeColumn);

                var searchCoumnID = search.createColumn({
                    name: 'formulanumeric',
                    formula: "{internalid}"
                });
                savedSearch.columns.push(searchCoumnID);

                if (hasMultibookFeature) {

                    var accountFilter = search.createFilter({
                        name: 'account',
                        join: 'accountingtransaction',
                        operator: search.Operator.ANYOF,
                        values: accountsIdArray
                    });
                    // log.debug('accountFilter 1', savedSearch.filters);
                    savedSearch.filters.splice(2, 0, accountFilter);

                    savedSearch.filters.splice(9, 0, accountFilter);

                    savedSearch.filters.splice(14, 0, accountFilter);
                    // log.debug('accountFilter 3', savedSearch.filters);
                    // var part1 = "CASE WHEN {accountingtransaction.account.id} in (";
                    // var part2 = accountsIdArray.join();
                    // var part3 = ") THEN 1 ELSE 0 END";

                    // var formulaes = part1 + part2 + part3;

                    // log.error("formula", formulaes);

                    var multibookFilter = search.createFilter({
                        name: "accountingbook",
                        join: "accountingtransaction",
                        operator: search.Operator.IS,
                        values: [paramMultibook]
                    });
                    savedSearch.filters.push(multibookFilter);

                    var amountFilter = search.createFilter({
                        name: 'formulatext',
                        operator: search.Operator.IS,
                        formula: 'CASE WHEN NVL({accountingtransaction.debitamount},0)-NVL({accountingtransaction.creditamount},0) = 0 THEN 0 ELSE 1 END',
                        values: '1'
                    });
                    savedSearch.filters.splice(3, 0, amountFilter);
                    savedSearch.filters.splice(8, 0, amountFilter);
                    savedSearch.filters.splice(15, 0, amountFilter);

                    ///11. INGRESOS BRUTOS RECIBIDOS POR OPERACIONES Propias.........................17
                    var searchColumn = search.createColumn({
                        name: 'formulacurrency',
                        formula: "CASE WHEN  (NVL({custbody_lmry_co_incomesource},'Operaciones propias')='Operaciones propias')  THEN NVL({accountingtransaction.creditamount},0) ELSE 0 END"
                    });
                    savedSearch.columns.push(searchColumn);

                    ///12. INGRESOS BRUTOS A TRAVES DE CONSORCIO O UNIONES TEMPORALES................18
                    var searchColumn2 = search.createColumn({
                        name: 'formulacurrency',
                        formula: "NVL({accountingtransaction.debitamount},0)"
                    });
                    savedSearch.columns.push(searchColumn2);

                    var searchColumn12 = search.createColumn({
                        name: 'formulanumeric',
                        formula: "{accountingtransaction.account.id}"
                    });
                    savedSearch.columns.push(searchColumn12);

                } else {

                    var amountFilter = search.createFilter({
                        name: 'formulatext',
                        operator: search.Operator.IS,
                        formula: 'CASE WHEN NVL({debitamount},0)-NVL({creditamount},0) = 0 THEN 0 ELSE 1 END',
                        values: '1'
                    });
                    savedSearch.filters.splice(3, 0, amountFilter);
                    savedSearch.filters.splice(10, 0, amountFilter);
                    savedSearch.filters.splice(14, 0, amountFilter);

                    var accountFilter = search.createFilter({
                        name: 'account',
                        operator: search.Operator.ANYOF,
                        values: accountsIdArray
                    });
                    savedSearch.filters.splice(4, 0, accountFilter);
                    savedSearch.filters.splice(11, 0, accountFilter);
                    savedSearch.filters.splice(15, 0, accountFilter);

                    var searchColumn12 = search.createColumn({
                        name: 'formulanumeric',
                        formula: "{account.id}"
                    });
                    savedSearch.columns.push(searchColumn12);

                }

                var info2Arr = [];
                if (accountsIdArray != null) {
                    var searchResult = savedSearch.run();
                    var calculoCuantia;
                    var columns;
                    while (!DbolStop) {
                        var objResult = searchResult.getRange(intDMinReg, intDMaxReg);

                        if (objResult != null) {
                            var intLength = objResult.length;
                            if (intLength < 1000) {
                                DbolStop = true;
                            }

                            // log.debug('intLength busqueda', intLength);
                            for (var i = 0; i < intLength; i++) {
                                var Arrtemporal = [];
                                var columns = objResult[i].columns;

                                if (Number(objResult[i].getValue(columns[1])) != 0 || Number(objResult[i].getValue(columns[2])) != 0 || Number(objResult[i].getValue(columns[7])) != 0 || Number(objResult[i].getValue(columns[8])) != 0) {
                                    // 1.- TIPO
                                    if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -') {
                                        var columna1 = objResult[i].getValue(columns[0]);
                                    } else {
                                        var columna1 = '';
                                    }

                                    // 2.- INGRESOS BRUTOS RECIBIDOS POR OPERACIONES PROPIAS

                                    if (hasMultibookFeature) {
                                        if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -')
                                            var columna2 = Math.abs(Number(objResult[i].getValue(columns[8]))).toFixed(2);

                                        else
                                            var columna2 = 0;
                                    } else {
                                        if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -')
                                            var columna2 = Math.abs(Number(objResult[i].getValue(columns[1]))).toFixed(2);
                                        else
                                            var columna2 = 0;
                                    }


                                    // 3.- DEVOLUCIONES, REBAJAS Y DESCUENTOS

                                    if (hasMultibookFeature) {
                                        if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -')
                                            var columna3 = Math.abs(Number(objResult[i].getValue(columns[9]))).toFixed(2);
                                        else
                                            var columna3 = 0;
                                    } else {
                                        if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -')
                                            var columna3 = Math.abs(Number(objResult[i].getValue(columns[2]))).toFixed(2);
                                        else
                                            var columna3 = 0;
                                    }


                                    // // 4.- CONCEPTO
                                    // if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -') {
                                    //     var columna4 = objResult[i].getValue(columns[3]);
                                    // } else {
                                    //     var columna4 = '';
                                    // }

                                    // 4.- VENDOR
                                    if (columna1 == 'ExpRept') {
                                        if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -') {
                                            var columna4 = ObtenerVendorExp(objResult[i].getValue(columns[7]));
                                        } else {
                                            var columna4 = '';
                                        }
                                    } else {
                                        if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -') {
                                            var columna4 = objResult[i].getValue(columns[3]);
                                        } else {
                                            var columna4 = '';
                                        }
                                    }

                                    // 5.- CUSTOMER
                                    if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -') {
                                        var columna5 = objResult[i].getValue(columns[4]);
                                    } else {
                                        var columna5 = '';
                                    }
                                    // 6.- employee  id
                                    if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -') {
                                        var columna6 = objResult[i].getValue(columns[5]);
                                    } else {
                                        var columna6 = '';
                                    }

                                    // 6.- TAX CODE
                                    if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -') {
                                        var taxcode = objResult[i].getValue(columns[6]);
                                    } else {
                                        var taxcode = '';
                                    }

                                    // 7.- ACCOUNT ID
                                    if (hasMultibookFeature) {
                                        if (objResult[i].getValue(columns[10]) != null && objResult[i].getValue(columns[10]) != '- None -') {
                                            var columna7 = objResult[i].getValue(columns[10]);
                                        } else {
                                            var columna7 = '';
                                        }
                                    } else {
                                        if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -') {
                                            var columna7 = objResult[i].getValue(columns[8]);
                                        } else {
                                            var columna7 = '';
                                        }
                                    }


                                    Arrtemporal = [columna1, columna2, columna3, columna4, columna5, columna6, columna7];
                                    // log.debug('Valores', columna1 + '-' + columna2 + '-' + columna3 + '-' + columna5 + '-' + columna6 + '-' + columna7);
                                    if (taxcode != 'UNDEF-CO') {
                                        info2Arr.push(Arrtemporal);
                                    }

                                }



                            }

                            if (!DbolStop) {
                                intDMinReg = intDMaxReg;
                                intDMaxReg += 1000;
                            }
                        } else {
                            DbolStop = true;
                        }
                    }
                    return info2Arr;
                } else {
                    log.debug('accountsIdArray', 'Es nullo');
                    return accountsIdArray;
                }



            } catch (e) {
                log.error('[ERROR EN BUSQUEDA PRINC]', e);
            }
        }

        function ObtieneIngresosCurrency1007() {
            try {
                var intDMinReg = 0;
                var intDMaxReg = 1000;

                var arrAuxiliar = [];
                var DbolStop = false;
                var contarRegistros = 0;

                // Consulta de Cuentas
                var savedSearch = search.load({
                    id: "customsearch_lmry_co_formag1007_curre_v9"
                });

                var accountsIdArray = ObtenerCuentas();

                if (hasSubsidiariaFeature) {
                    var subsidiaryFilter = search.createFilter({
                        name: "subsidiary",
                        operator: search.Operator.IS,
                        values: [paramSubsidiaria]
                    });
                    savedSearch.filters.push(subsidiaryFilter);
                }

                if (paramPeriodo) {
                    var periodStartDateFilter = search.createFilter({
                        name: "trandate",
                        operator: search.Operator.ONORAFTER,
                        values: [periodStartDate]
                    });
                    savedSearch.filters.push(periodStartDateFilter);

                    var periodEndDateFilter = search.createFilter({
                        name: "trandate",
                        operator: search.Operator.ONORBEFORE,
                        values: [periodEndDate]
                    });
                    savedSearch.filters.push(periodEndDateFilter);
                }

                var vendorColumn = search.createColumn({
                    name: 'formulanumeric',
                    summary: 'GROUP',
                    formula: "{vendorline.internalid}"
                });
                savedSearch.columns.push(vendorColumn);


                if ((hasJobFeature && !isAdvanceJobsFeature)) {
                    log.debug('entra con jobs', 'si jobs');
                    var cuitjobs = search.createColumn({
                        name: "formulanumeric",
                        formula: "CASE WHEN NVL({job.internalid},-1) = -1 THEN {customer.internalid} ELSE {job.customer.id} end",
                        summary: "GROUP"
                    });
                    savedSearch.columns.push(cuitjobs);

                } else {
                    log.debug('entra NO jobs', 'NO jobs');

                    var cuitjobs = search.createColumn({
                        name: 'formulanumeric',
                        formula: '{customer.internalid}',
                        summary: 'GROUP'
                    });
                    savedSearch.columns.push(cuitjobs);
                }

                var searchColumndif = search.createColumn({
                    name: 'formulanumeric',
                    formula: "NVL({debitamount},0)-NVL({creditamount},0)",
                    summary: 'SUM'
                });
                savedSearch.columns.push(searchColumndif);

                var searchCoumnID = search.createColumn({
                    name: 'formulanumeric',
                    formula: "{internalid}",
                    summary: 'GROUP'
                });
                savedSearch.columns.push(searchCoumnID);

                if (hasMultibookFeature) {

                    var accountFilter = search.createFilter({
                        name: 'account',
                        join: 'accountingtransaction',
                        operator: search.Operator.ANYOF,
                        values: accountsIdArray
                    });
                    savedSearch.filters.splice(4, 0, accountFilter);


                    var multibookFilter = search.createFilter({
                        name: "accountingbook",
                        join: "accountingtransaction",
                        operator: search.Operator.IS,
                        values: [paramMultibook]
                    });
                    savedSearch.filters.push(multibookFilter);

                    var amountFilter = search.createFilter({
                        name: 'formulatext',
                        operator: search.Operator.IS,
                        formula: 'CASE WHEN NVL({accountingtransaction.debitamount},0)-NVL({accountingtransaction.creditamount},0) = 0 THEN 0 ELSE 1 END',
                        values: '1'
                    });
                    savedSearch.filters.splice(5, 1, amountFilter);

                    ///11. INGRESOS BRUTOS RECIBIDOS POR OPERACIONES Propias.........................17
                    var searchColumn = search.createColumn({
                        name: 'formulacurrency',
                        formula: "CASE WHEN  (NVL({custbody_lmry_co_incomesource},'Operaciones propias')='Operaciones propias')  THEN NVL({accountingtransaction.creditamount},0) ELSE 0 END",
                        summary: 'SUM'
                    });
                    savedSearch.columns[1] = searchColumn;

                    ///12. INGRESOS BRUTOS A TRAVES DE CONSORCIO O UNIONES TEMPORALES................18
                    var searchColumn2 = search.createColumn({
                        name: 'formulacurrency',
                        formula: "NVL({accountingtransaction.debitamount},0)",
                        summary: 'SUM'
                    });
                    savedSearch.columns[2] = searchColumn2;

                    var searchColumn12 = search.createColumn({
                        name: 'formulanumeric',
                        formula: "{accountingtransaction.account.id}",
                        summary: 'GROUP'
                    });
                    savedSearch.columns.push(searchColumn12);

                } else {
                    var accountFilter = search.createFilter({
                        name: 'account',
                        operator: search.Operator.ANYOF,
                        values: accountsIdArray
                    });
                    savedSearch.filters.splice(3, 0, accountFilter);

                    var searchColumn12 = search.createColumn({
                        name: 'formulanumeric',
                        formula: "{account.id}",
                        summary: 'GROUP'
                    });
                    savedSearch.columns.push(searchColumn12);

                }

                var info2Arr = [];
                if (accountsIdArray != null) {
                    var searchResult = savedSearch.run();
                    var calculoCuantia;
                    var columns;
                    while (!DbolStop) {
                        var objResult = searchResult.getRange(intDMinReg, intDMaxReg);

                        if (objResult != null) {
                            var intLength = objResult.length;
                            if (intLength < 1000) {
                                DbolStop = true;
                            }

                            for (var i = 0; i < intLength; i++) {
                                var Arrtemporal = [];
                                var columns = objResult[i].columns;

                                if (Number(objResult[i].getValue(columns[1]) != 0 || Number(objResult[i].getValue(columns[2])) != 0)) {
                                    // 1.- TIPO
                                    if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -') {
                                        var columna1 = objResult[i].getValue(columns[0]);
                                    } else {
                                        var columna1 = '';
                                    }

                                    // 2.- INGRESOS BRUTOS RECIBIDOS POR OPERACIONES PROPIAS


                                    if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -')
                                        var columna2 = Math.abs(Number(objResult[i].getValue(columns[1]))).toFixed(2);
                                    else
                                        var columna2 = 0;



                                    // 3.- DEVOLUCIONES, REBAJAS Y DESCUENTOS
                                    if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -')
                                        var columna3 = Math.abs(Number(objResult[i].getValue(columns[2]))).toFixed(2);
                                    else
                                        var columna3 = 0;

                                    // 4.- VENDOR
                                    if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -') {
                                        var columna4 = objResult[i].getValue(columns[3]);
                                    } else {
                                        var columna4 = '';
                                    }
                                    // 4.- CUSTOMER
                                    if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -') {
                                        var columna5 = objResult[i].getValue(columns[4]);
                                    } else {
                                        var columna5 = '';
                                    }


                                    // 4.- DIFERENCIA
                                    if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -') {
                                        var columna6 = objResult[i].getValue(columns[5]);
                                    } else {
                                        var columna6 = '';
                                    }


                                    // 4.- internal id Currency
                                    if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -') {
                                        var columna8 = objResult[i].getValue(columns[6]);
                                    } else {
                                        var columna8 = '';
                                    }

                                    //CONCEPTO
                                    if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -')
                                        var columna7 = objResult[i].getValue(columns[7]);
                                    else
                                        var columna7 = '';


                                    Arrtemporal = [columna1, columna2, columna3, columna4, columna5, '', columna7, columna8];

                                    info2Arr.push(Arrtemporal);

                                }
                            }

                            if (!DbolStop) {
                                intDMinReg = intDMaxReg;
                                intDMaxReg += 1000;
                            }
                        } else {
                            DbolStop = true;
                        }
                    }

                    return info2Arr;
                } else {
                    return accountsIdArray;
                }


            } catch (e) {
                log.error('[ERROR EN BUSQUEDA CURRENCY]', e);
            }
        }

        function ObtenerCuentas() {

            var savedSearch = search.create({
                type: "account",
                filters: [
                    ["custrecord_lmry_co_puc_formatgy", "anyof", "5"],
                    "AND", ["custrecord_lmry_co_puc_concept.custrecord_lmry_co_puc_formatid_c", "is", "1007"]
                ],
                columns: [
                    "internalid",
                    "type"
                ]
            });

            var accountJson = {};
            var objResult = savedSearch.run().getRange(0, 1000);
            var aux = [];
            // log.debug('Cuentas Busqueda', objResult);

            if (objResult && objResult.length) {
                var columns;
                for (var i = 0; i < objResult.length; i++) {
                    columns = objResult[i].columns;
                    aux.push(objResult[i].getValue(columns[1]));
                    accountJson[objResult[i].getValue(columns[0])] = aux;
                    aux = [];
                }
                var accountsIdArray = Object.keys(accountJson);
                // log.debug('accountsIdArray', accountsIdArray);
                return accountsIdArray;
            }

        }

        function ObtieneIngresosJournal1007() {
            try {
                var intDMinReg = 0;
                var intDMaxReg = 1000;

                var arrAuxiliar = [];
                var DbolStop = false;
                var contarRegistros = 0;

                // Consulta de Cuentas
                var savedSearch = search.load({
                    id: "customsearch_lmry_co_formag1007_journ_v9"
                });

                var accountsIdArray = ObtenerCuentas();

                if (hasSubsidiariaFeature) {
                    var subsidiaryFilter = search.createFilter({
                        name: "subsidiary",
                        operator: search.Operator.IS,
                        values: [paramSubsidiaria]
                    });
                    savedSearch.filters.push(subsidiaryFilter);
                }

                if (paramPeriodo) {
                    var periodStartDateFilter = search.createFilter({
                        name: "trandate",
                        operator: search.Operator.ONORAFTER,
                        values: [periodStartDate]
                    });
                    savedSearch.filters.push(periodStartDateFilter);

                    var periodEndDateFilter = search.createFilter({
                        name: "trandate",
                        operator: search.Operator.ONORBEFORE,
                        values: [periodEndDate]
                    });
                    savedSearch.filters.push(periodEndDateFilter);
                }

                var vendorColumn = search.createColumn({
                    name: 'formulanumeric',
                    summary: 'GROUP',
                    formula: "{vendor.internalid}"
                });
                savedSearch.columns.push(vendorColumn);


                if ((hasJobFeature && !isAdvanceJobsFeature)) {
                    log.debug('entra con jobs', 'si jobs');
                    var cuitjobs = search.createColumn({
                        name: "formulanumeric",
                        formula: "CASE WHEN NVL({job.internalid},-1) = -1 THEN {customer.internalid} ELSE {job.customer.id} end",
                        summary: "GROUP"
                    });
                    savedSearch.columns.push(cuitjobs);

                } else {
                    log.debug('entra NO jobs', 'NO jobs');

                    var cuitjobs = search.createColumn({
                        name: 'formulanumeric',
                        formula: '{customer.internalid}',
                        summary: 'GROUP'
                    });
                    savedSearch.columns.push(cuitjobs);
                }

                var employeeColumn = search.createColumn({
                    name: 'formulanumeric',
                    summary: 'GROUP',
                    formula: "{entity.id}"
                });
                savedSearch.columns.push(employeeColumn);

                var TaxcodeColumn = search.createColumn({
                    name: 'formulatext',
                    summary: 'GROUP',
                    formula: "{taxcode}"
                });
                savedSearch.columns.push(TaxcodeColumn);

                if (hasMultibookFeature) {

                    var accountFilter = search.createFilter({
                        name: 'account',
                        join: 'accountingtransaction',
                        operator: search.Operator.ANYOF,
                        values: accountsIdArray
                    });
                    savedSearch.filters.splice(1, 0, accountFilter);


                    var multibookFilter = search.createFilter({
                        name: "accountingbook",
                        join: "accountingtransaction",
                        operator: search.Operator.IS,
                        values: [paramMultibook]
                    });
                    savedSearch.filters.push(multibookFilter);

                    ///11. INGRESOS BRUTOS RECIBIDOS POR OPERACIONES Propias.........................17
                    var searchColumn = search.createColumn({
                        name: 'formulacurrency',
                        formula: "CASE WHEN  (NVL({custbody_lmry_co_incomesource},'Operaciones propias')='Operaciones propias')  THEN NVL({accountingtransaction.creditamount},0) ELSE 0 END",
                        summary: 'SUM'
                    });
                    savedSearch.columns.push(searchColumn);

                    ///12. INGRESOS BRUTOS A TRAVES DE CONSORCIO O UNIONES TEMPORALES................18
                    var searchColumn2 = search.createColumn({
                        name: 'formulacurrency',
                        formula: "NVL({accountingtransaction.debitamount},0)",
                        summary: 'SUM'
                    });
                    savedSearch.columns.push(searchColumn2);

                    var searchColumn12 = search.createColumn({
                        name: 'formulanumeric',
                        formula: "{accountingtransaction.account.id}",
                        summary: 'GROUP'
                    });
                    savedSearch.columns.push(searchColumn12);

                } else {
                    var accountFilter = search.createFilter({
                        name: 'account',
                        operator: search.Operator.ANYOF,
                        values: accountsIdArray
                    });
                    savedSearch.filters.splice(3, 0, accountFilter);

                    var searchColumn12 = search.createColumn({
                        name: 'formulanumeric',
                        formula: "{account.id}",
                        summary: 'GROUP'
                    });
                    savedSearch.columns.push(searchColumn12);

                }

                var info2Arr = [];
                if (accountsIdArray != null) {
                    var searchResult = savedSearch.run();
                    var calculoCuantia;
                    var columns;
                    while (!DbolStop) {
                        var objResult = searchResult.getRange(intDMinReg, intDMaxReg);

                        if (objResult != null) {
                            var intLength = objResult.length;
                            if (intLength < 1000) {
                                DbolStop = true;
                            }

                            for (var i = 0; i < intLength; i++) {
                                var Arrtemporal = [];

                                var columns = objResult[i].columns;

                                if (Number(objResult[i].getValue(columns[1]) != 0 || Number(objResult[i].getValue(columns[2])) != 0)) {
                                    // 1.- TIPO
                                    if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -') {
                                        var columna1 = objResult[i].getValue(columns[0]);
                                    } else {
                                        var columna1 = '';
                                    }

                                    // 2.- INGRESOS BRUTOS RECIBIDOS POR OPERACIONES PROPIAS

                                    if (hasMultibookFeature) {
                                        if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -')
                                            var columna2 = Math.abs(Number(objResult[i].getValue(columns[7]))).toFixed(2);
                                        else
                                            var columna2 = 0;
                                    } else {
                                        if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -')
                                            var columna2 = Math.abs(Number(objResult[i].getValue(columns[1]))).toFixed(2);
                                        else
                                            var columna2 = 0;
                                    }


                                    // 3.- DEVOLUCIONES, REBAJAS Y DESCUENTOS

                                    if (hasMultibookFeature) {
                                        if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -')
                                            var columna3 = Math.abs(Number(objResult[i].getValue(columns[8]))).toFixed(2);
                                        else
                                            var columna3 = 0;
                                    } else {
                                        if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -')
                                            var columna3 = Math.abs(Number(objResult[i].getValue(columns[2]))).toFixed(2);
                                        else
                                            var columna3 = 0;
                                    }


                                    // 4.- VENDOR
                                    if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -') {
                                        var columna4 = objResult[i].getValue(columns[3]);
                                    } else {
                                        var columna4 = '';
                                    }
                                    // 4.- CUSTOMER
                                    if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -') {
                                        var columna5 = objResult[i].getValue(columns[4]);
                                    } else {
                                        var columna5 = '';
                                    }
                                    // 4.- EMPLOYEE
                                    if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -') {
                                        var columna6 = objResult[i].getValue(columns[5]);
                                    } else {
                                        var columna6 = '';
                                    }

                                    // 4.- EMPLOYEE
                                    if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -') {
                                        var taxcode = objResult[i].getValue(columns[6]);
                                    } else {
                                        var taxcode = '';
                                    }

                                    //CONCEPTO
                                    if (hasMultibookFeature) {
                                        if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -')
                                            var columna7 = objResult[i].getValue(columns[9]);
                                        else
                                            var columna7 = '';
                                    } else {
                                        if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -')
                                            var columna7 = objResult[i].getValue(columns[7]);
                                        else
                                            var columna7 = '';
                                    }

                                }
                                if (taxcode != 'UNDEF-CO') {
                                    Arrtemporal = [columna1, columna2, columna3, columna4, columna5, columna6, columna7];
                                    // log.debug('Arrtemporal Journal', Arrtemporal);
                                    info2Arr.push(Arrtemporal);
                                }

                            }

                            if (!DbolStop) {
                                intDMinReg = intDMaxReg;
                                intDMaxReg += 1000;
                            }
                        } else {
                            DbolStop = true;
                        }
                    }

                    return info2Arr;
                } else {
                    return accountsIdArray;
                }


            } catch (e) {
                log.error('[ERROR EN BUSQUEDA JOURNAL]', e);
            }
        }


        function NoData(exerror) {
            var usuarioTemp = runtime.getCurrentUser();
            var id = usuarioTemp.id;
            var employeename = search.lookupFields({
                type: search.Type.EMPLOYEE,
                id: id,
                columns: ['firstname', 'lastname']
            });
            var usuario = employeename.firstname + ' ' + employeename.lastname;

            if (Number(paramCont) > 1) {
                var recordLog = record.load({
                    type: 'customrecord_lmry_co_rpt_generator_log',
                    id: paramIdLog
                });
            } else {
                var recordLog = record.load({
                    type: 'customrecord_lmry_co_rpt_generator_log',
                    id: paramIdLog
                });
            }

            switch (exerror) {
                case '1':
                    var mensaje = 'No existe informacion para los criterios seleccionados.';
                    break;
                case '2':
                    var mensaje = 'Ocurrio un error inesperado en la ejecucion del reporte.';
                    break;
            }

            //Nombre de Archivo
            recordLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_name',
                value: mensaje
            });

            //Creado Por
            recordLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_employee',
                value: usuario
            });

            var recordId = recordLog.save();
        }

        function GenerarExcel(ingresosRecibidosArray, numeroEnvio) {
            try {
                var xlsString = '';

                //PDF Normalization
                var todays = parseDateTo(new Date(), "DATE");
                var currentTime = getTimeHardcoded(parseDateTo(new Date(), "DATETIME"));

                //cabecera de excel
                xlsString = '<?xml version="1.0" encoding="UTF-8" ?><?mso-application progid="Excel.Sheet"?>';
                xlsString += '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" ';
                xlsString += 'xmlns:o="urn:schemas-microsoft-com:office:office" ';
                xlsString += 'xmlns:x="urn:schemas-microsoft-com:office:excel" ';
                xlsString += 'xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet" ';
                xlsString += 'xmlns:html="http://www.w3.org/TR/REC-html40">';
                xlsString += '<Styles>';
                xlsString += '<Style ss:ID="s21"><Font ss:Bold="1" ss:Size="12" /><Alignment ss:Horizontal="Center" ss:Vertical="Bottom"/></Style>';
                xlsString += '<Style ss:ID="s22"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom"/></Style>';
                xlsString += '<Style ss:ID="s23"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom"/><NumberFormat ss:Format="_(* ###0_);_(* \(###0\);_(@_)"/></Style>';
                xlsString += '<Style ss:ID="s24"><NumberFormat ss:Format="_(* ###0_);_(* \(###0\);_(@_)"/></Style>';
                xlsString += '</Styles><Worksheet ss:Name="Sheet1">';


                xlsString += '<Table>';
                xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
                xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
                xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
                xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
                xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
                xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
                xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
                xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
                xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
                xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
                xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';

                //Cabecera
                xlsString += '<Row>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS["titulo"][language] + '</Data></Cell>';
                xlsString += '</Row>';
                xlsString += '<Row></Row>';
                xlsString += '<Row>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS["razSocial"][language] + ': ' + companyName + '</Data></Cell>';
                xlsString += '</Row>';
                xlsString += '<Row>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS["taxNumber"][language] + ': ' + companyRuc + '</Data></Cell>';
                xlsString += '</Row>';
                xlsString += '<Row>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS["period"][language] + ': ' + formato_stardate + GLOBAL_LABELS["al"][language] + formato_enddate + '</Data></Cell>';
                xlsString += '</Row>';

                if (hasMultibookFeature || hasMultibookFeature == 'T') {
                    xlsString += '<Row>';
                    xlsString += '<Cell></Cell>';
                    xlsString += '<Cell></Cell>';
                    xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS["libCont"][language] + ': ' + multibookName + '</Data></Cell>';
                    xlsString += '</Row>';
                }

                // PDF Normalization

                xlsString += '<Row>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS["origin"][language] + ': Netsuite' + '</Data></Cell>';
                xlsString += '</Row>';
                xlsString += '<Row>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS["date"][language] + ': ' + todays + '</Data></Cell>';
                xlsString += '</Row>';
                xlsString += '<Row>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS["time"][language] + ': ' + currentTime + '</Data></Cell>';
                xlsString += '</Row>';

                // END PDF Normalization

                xlsString += '<Row></Row>';
                xlsString += '<Row></Row>';
                xlsString += '<Row>' +
                    '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS["cpt"][language] + ' </Data></Cell>' +
                    '<Cell ss:StyleID="s21"><Data ss:Type="String"> TDOC </Data></Cell>' +
                    '<Cell ss:StyleID="s21"><Data ss:Type="String"> NID </Data></Cell>' +
                    '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS["primerApe"][language] + ' </Data></Cell>' +
                    '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS["segundApe"][language] + ' </Data></Cell>' +
                    '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS["primerNom"][language] + ' </Data></Cell>' +
                    '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS["segundNom"][language] + ' </Data></Cell>' +
                    '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS["razSocial"][language] + ' </Data></Cell>' +
                    '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS["pais"][language] + ' </Data></Cell>' +
                    '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS["ingresoBruto"][language] + ' </Data></Cell>' +
                    '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS["devolRebaj"][language] + ' </Data></Cell>' +
                    '</Row>';

                //creacion de reporte xls
                for (var ii = 0; ii < ingresosRecibidosArray.length; ii++) {
                    if (ingresosRecibidosArray[ii][9] > 0 || ingresosRecibidosArray[ii][10] > 0) {

                        xlsString += '<Row>';
                        //0. CONCEPTO
                        if (ingresosRecibidosArray[ii][0] != '' || ingresosRecibidosArray[ii][0] != null) {
                            if (ingresosRecibidosArray[ii][0] != '- None -')
                                xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][0] + '</Data></Cell>';
                            else
                                xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        } else {
                            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        }

                        //1. TDOC
                        if (ingresosRecibidosArray[ii][1] != '' || ingresosRecibidosArray[ii][1] != null) {
                            if (ingresosRecibidosArray[ii][1] != '- None -')
                                xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][1] + '</Data></Cell>';
                            else
                                xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        } else {
                            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        }

                        //2. NID
                        if (ingresosRecibidosArray[ii][2] != '' || ingresosRecibidosArray[ii][2] != null) {
                            if (ingresosRecibidosArray[ii][2] != '- None -') {
                                ingresosRecibidosArray[ii][2] = ingresosRecibidosArray[ii][2];
                                xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][2] + '</Data></Cell>';
                            } else
                                xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        } else {
                            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        }

                        //3. 1ER APELL
                        if (ingresosRecibidosArray[ii][3] && ingresosRecibidosArray[ii][3]) {
                            xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][3] + '</Data></Cell>';
                        } else {
                            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        }

                        //4. 2DO APELL
                        if (ingresosRecibidosArray[ii][4] && ingresosRecibidosArray[ii][4]) {
                            xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][4] + '</Data></Cell>';
                        } else {
                            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        }

                        //5. 1ER NOMBRE
                        if (ingresosRecibidosArray[ii][5] && ingresosRecibidosArray[ii][5]) {
                            xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][5] + '</Data></Cell>';
                        } else {
                            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        }

                        //6. 2DO NOMBRE
                        if (ingresosRecibidosArray[ii][6] && ingresosRecibidosArray[ii][6]) {
                            xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][6] + '</Data></Cell>';
                        } else {
                            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        }

                        //7. RAZON SOCIAL
                        if (ingresosRecibidosArray[ii][7] != '' || ingresosRecibidosArray[ii][7] != null) {
                            if (ingresosRecibidosArray[ii][7] != '- None -') {
                                ingresosRecibidosArray[ii][7] = ingresosRecibidosArray[ii][7];
                                xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][7] + '</Data></Cell>';
                            } else
                                xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        } else {
                            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        }

                        //8. PAIS
                        if (ingresosRecibidosArray[ii][8] != '' || ingresosRecibidosArray[ii][8] != null) {
                            if (ingresosRecibidosArray[ii][8] != '- None -') {
                                xlsString += '<Cell><Data ss:Type="String">' + ingresosRecibidosArray[ii][8] + '</Data></Cell>';
                            } else
                                xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        } else {
                            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        }

                        //9. INGRESOS BRUTOS RECIBIDOS POR OPERACIONES PROPIAS
                        if (ingresosRecibidosArray[ii][9] != '' || ingresosRecibidosArray[ii][9] != null) {
                            if (ingresosRecibidosArray[ii][9] != '- None -')
                                xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + parseFloat(ingresosRecibidosArray[ii][9]).toFixed(0) + '</Data></Cell>';
                            else
                                xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">0</Data></Cell>';
                        } else {
                            xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">0</Data></Cell>';
                        }

                        //10. DEVOLUCIONES, REBAJAS Y DESCUENTOS
                        if (ingresosRecibidosArray[ii][10] != '' || ingresosRecibidosArray[ii][10] != null)
                            if (ingresosRecibidosArray[ii][10] != '- None -')
                                xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + parseFloat(ingresosRecibidosArray[ii][10]).toFixed(0) + '</Data></Cell>';
                            else
                                xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">0</Data></Cell>';
                        else {
                            xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">0</Data></Cell>';
                        }
                        xlsString += '</Row>';
                    }
                } //fin del quiebre por clase

                xlsString += '</Table></Worksheet></Workbook>';

                //Se arma el archivo EXCEL
                strExcelIngresosRecibidos = encode.convert({
                    string: xlsString,
                    inputEncoding: encode.Encoding.UTF_8,
                    outputEncoding: encode.Encoding.BASE_64
                });

                // numeroEnvio = obtenerNumeroEnvio();
                // var nameFile = "Dmuisca" + completar_cero(2, paramConcepto) + '01007' + '09' + paramPeriodo + completar_cero(8, numeroEnvio);

                SaveFile('.xls', strExcelIngresosRecibidos, numeroEnvio);
            } catch (error) {
                log.error('[ERROR EXCEL]', error);
            }

        }

        function getGlobalLabels() {
            var labels = {
                "titulo": {
                    "es": 'FORMULARIO 1007: INGRESOS RECIBIDOS',
                    "pt": 'FORMA 1007: RENDA RECEBIDA',
                    "en": 'FORM 1007: INCOME RECEIVED'
                },
                "razSocial": {
                    "es": 'Razon Social',
                    "pt": 'Razão social',
                    "en": 'Company name'
                },
                "taxNumber": {
                    "es": 'Numero de Impuesto',
                    "pt": 'Número de identificação fiscal',
                    "en": 'Tax Number'
                },
                "period": {
                    "es": 'Periodo',
                    "pt": 'Período',
                    "en": 'Period'
                },
                "al": {
                    "es": ' al ',
                    "pt": ' a ',
                    "en": ' to '
                },
                "libCont": {
                    "es": 'Libro Contable',
                    "pt": 'Livro de contabilidade',
                    "en": 'Book Accounting'
                },
                "cpt": {
                    "es": 'Concepto',
                    "pt": 'Concepto',
                    "en": 'Concept'
                },
                "primerApe": {
                    "es": '1er Apelli',
                    "pt": '1er Apelli',
                    "en": '1st Last Name'
                },
                "segundApe": {
                    "es": '2do Apelli',
                    "pt": '2do Apelli',
                    "en": '2nd Last Name'
                },
                "primerNom": {
                    "es": '1er Nombre',
                    "pt": '1er Nombre',
                    "en": '1st Name'
                },
                "segundNom": {
                    "es": '2do Nombre',
                    "pt": '2do Nombre',
                    "en": '2st Name'
                },
                "pais": {
                    "es": 'Pais',
                    "pt": 'Pais',
                    "en": 'Country'
                },
                "ingresoBruto": {
                    "es": 'Ingr. Bruto Recibido x Op.Propias',
                    "pt": 'Ingr. Bruto Recibido x Op.Propias',
                    "en": 'Gross Ticket Received x Op. Own'
                },
                "devolRebaj": {
                    "es": 'Devol. Rebajas, Descuento',
                    "pt": 'Devol. Rebajas, Descuento',
                    "en": 'Devol. Reductions, Discount'
                },
                "origin": {
                    "es": 'Origen',
                    "pt": 'Origem',
                    "en": 'Origin'
                },
                "date": {
                    "es": 'Fecha',
                    "pt": 'Data',
                    "en": 'Date'
                },
                "time": {
                    "es": 'Hora',
                    "pt": 'Hora',
                    "en": 'Time'
                }


            };

            return labels;
        }

        function parseDateTo(trandate, type) {
            var $date = '';
    
            if (!trandate) return;
    
            // In Scheduled or Map/Reduce scripts the user timezone is not available
            var userObj = runtime.getCurrentUser();
            var userPrefTime = userObj.getPreference({ name: 'TIMEZONE' });
    
            $date = format.format({ value: trandate, type: format.Type[type], timezone: userPrefTime });
    
            return $date;
        }
        
        //** Function used to Get Current Time by only DAYTIME*/
        function getTimeHardcoded(datetime){
    
            if (!datetime) return;
    
            // This is provider by NetSuite Settings > User Preferences > Time Format
            var timeFormat = {
                "h:mm a": ":",
                "H:mm": ":",
                "h-mm a": "-",
                "H-mm": "-",
            }
    
            var userObj = runtime.getCurrentUser();
            var userPrefTimeFormat = userObj.getPreference({ name: 'TIMEFORMAT' });
    
            var separator = timeFormat[userPrefTimeFormat];
    
            var time = datetime.split(" ")[1];
            var ampm = datetime.split(" ")[2];
    
            var hours = time.split(separator)[0];
            var minutes = time.split(separator)[1];
    
            var time_ampm = hours + separator + minutes + " " + ampm;
            time = hours + separator + minutes;
    
            return  (ampm) ? time_ampm : time;
        }

        return {
            getInputData: getInputData,
            map: map,
            reduce: reduce,
            summarize: summarize
        };

    });