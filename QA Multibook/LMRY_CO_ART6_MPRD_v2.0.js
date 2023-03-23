/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_ART6_MPRD_v2.0.js            ||
||                                                              ||
||  Version Date         Author        Remarks                  ||
||  2.0     Sep 06 2020  LatamReady    Use Script 2.0           ||
\= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.0
 * @NScriptType MapReduceScript
 * @NModuleScope Public
 */
define(['N/search', 'N/log', "N/config", 'require', 'N/file', 'N/runtime', 'N/query', "N/format", "N/record", "N/task", "./CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js", "/SuiteBundles/Bundle 37714/Latam_Library/LMRY_LibraryReport_LBRY_V2.js"],

    function (search, log, config, require, fileModulo, runtime, query, format, recordModulo, task, libreria, libReport) {

        /**
         * Input Data for processing
         *
         * @return Array,Object,Search,File
         *
         * @since 2016.1
         */

        var objContext = runtime.getCurrentScript();

        var LMRY_script = "LMRY_CO_ART6_MPRD_v2.0.js";

        var objContext = runtime.getCurrentScript();

        var companyruc = '';

        //Parametros
        param_RecorID = objContext.getParameter({
            name: 'custscript_lmry_co_art6_recid'
        });
        param_Periodo = objContext.getParameter({
            name: 'custscript_lmry_co_art6_period'
        });
        param_Anual = objContext.getParameter({
            name: 'custscript_lmry_co_art6_anual'
        });
        param_Multi = objContext.getParameter({
            name: 'custscript_lmry_co_art6_mutibook'
        });
        param_FeatID = objContext.getParameter({
            name: 'custscript_lmry_co_art6_featid'
        });
        param_Subsi = objContext.getParameter({
            name: 'custscript_lmry_co_art6_subsi'
        });
        param_head = objContext.getParameter({
            name: 'custscript_lmry_co_art6_inserthead'
        });

        //************FEATURES********************
        feature_Subsi = runtime.isFeatureInEffect({
            feature: "SUBSIDIARIES"
        });
        feature_Multi = runtime.isFeatureInEffect({
            feature: "MULTIBOOK"
        });
        Feature_Lote = runtime.isFeatureInEffect({
            feature: 'LOTNUMBEREDINVENTORY'
        });
        hasJobsFeature = runtime.isFeatureInEffect({
            feature: "JOBS"
        });
        hasAdvancedJobsFeature = runtime.isFeatureInEffect({
            feature: "ADVANCEDJOBS"
        });

        var hasMulticalendarFeature = runtime.isFeatureInEffect({
            feature: 'MULTIPLECALENDARS'
        });

        var featureSpecialPeriod = null;

        var language = runtime.getCurrentScript().getParameter({
            name: 'LANGUAGE'
        }).substring(0, 2);

        if (language != "en" && language != "es" && language != "pt") {
            language = "es";
        }

        function getInputData() {
            try {
                log.debug('parametros', param_Multi + '-' + param_Subsi + '-' + param_Anual);
                log.debug('features:', feature_Subsi + '-' + feature_Multi + '-' + hasMulticalendarFeature);

                var whtLines = getWHTLines();
                log.debug('whtLines', whtLines);
                var whtCabecera = getWHTCabecera();
                log.debug('whtCabecera', whtCabecera);
                // var whtJournal = getWHTJournal();
                // log.debug('whtJournal', whtJournal);
                var whtTotal = whtLines.concat(whtCabecera);
                // var whtTotal = whtLines.concat(whtCabecera, whtJournal);
                log.debug('whtTotal', whtTotal);
                return whtTotal;

            } catch (err) {
                log.error('err', err);
                libreria.sendMail(LMRY_script, ' [ getInputData ] ' + err);
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
                var ArrCustomer = new Array();
                var arrTemp = JSON.parse(context.value);
                var montoBase = 0;
                var alicuota = 0;
                var retencion = 0;

                if (arrTemp[0] == 'Journal') {
                    var entityData = getCustomerData(arrTemp[3]);

                    if (entityData != null) {
                        var addressData = getCustAddressData(arrTemp[3]);
                        addressData = addressData.split('|');

                        var taxResults = getTaxResults(arrTemp[1], arrTemp[2]);

                        if (taxResults.length != 0) {
                            montoBase = taxResults[0][0];
                            alicuota = taxResults[0][2];
                            retencion = taxResults[0][1];

                            id_reduce = arrTemp[3] + '|' + alicuota; //ID VENDOR + ALIQUOTA

                            ArrCustomer = [entityData[0], entityData[1], entityData[2], entityData[3], entityData[4], addressData[0],
                            addressData[1], addressData[2]
                            ];

                        } else {
                            //log.debug('No hay taxresult en journal');
                            return false;
                        }

                    } else {
                        return false;
                    }

                } else {
                    var entityData = getCustomerData(arrTemp[0]);
                    //log.debug('entityData',entityData);
                    var addressData = getCustAddressData(arrTemp[0]);
                    addressData = addressData.split('|');

                    montoBase = arrTemp[1];
                    alicuota = arrTemp[2];
                    retencion = arrTemp[3];

                    ArrCustomer = [entityData[0], entityData[1], entityData[2], entityData[3], entityData[4], addressData[0],
                    addressData[1], addressData[2]
                    ];

                    id_reduce = arrTemp[0] + '|' + arrTemp[2]; //id customer + alicuota
                }

                context.write({
                    key: id_reduce,
                    value: {
                        Customer: ArrCustomer,
                        Montobase: montoBase,
                        Aliquota: alicuota,
                        MontoRetenido: retencion
                    }
                });
            } catch (err) {
                log.error('err', err);
            }
        }

        /**
         * If this entry point is used, the reduce function is invoked one time for
         * each key and list of values provided..
         *
         * @param {Object} context
         * @param {boolean} context.isRestarted - Indicates whether the current invocation of the represents a restart.
         * @param {number} context.concurrency - The maximum concurrency number when running the map/reduce script.
         * @param {Date} 0context.datecreated - The time and day when the script began running.
         * @param {number} context.seconds - The total number of seconds that elapsed during the processing of the script.
         * @param {number} context.usage - TThe total number of usage units consumed during the processing of the script.
         * @param {number} context.yields - The total number of yields that occurred during the processing of the script.
         * @param {Object} context.inputSummary - Object that contains data about the input stage.
         * @param {Object} context.mapSummary - Object that contains data about the map stage.
         * @param {Object} context.reduceSummary - Object that contains data about the reduce stage.
         * @param {Iterator} context.output - This param contains a "iterator().each(parameters)" function
         *
         * @since 2016.1
         */

        function reduce(context) {
            try {
                var estado;
                var monto = 0;
                var ArrCustomer = new Array();
                var ArrItem = new Array();
                var monto_B = 0;
                var monto_R = 0;
                var por = '';
                var arreglo = context.values;
                var tamaño = arreglo.length;
                for (var i = 0; i < tamaño; i++) {
                    var obj = JSON.parse(arreglo[i]);
                    /*if (obj["isError"] == "T") {
                        context.write({
                            key   : context.key,
                            value : obj
                        });
                        return;
                    }*/

                    ArrCustomer = obj.Customer;

                    monto_B += obj.Montobase;
                    monto_R += obj.MontoRetenido;
                    por = Number(obj.Aliquota); //*10000
                    por = por * 10;
                    por = por.toFixed(2);
                    if (por == 0) {
                        por = '0.00'
                    }

                }

                monto_B = redondear(monto_B);
                monto_R = redondear(monto_R);

                if (monto_B != 0 && monto_R != 0) {
                    context.write({
                        key: context.key,
                        value: {
                            Customer: ArrCustomer,
                            Montobase: monto_B,
                            Aliquota: por,
                            MontoRetenido: monto_R
                        }
                    });
                }
            } catch (error) {
                log.error('err reduce', error);
            }
        }

        function summarize(context) {

            try {
                strReporte = '';
                featureSpecialPeriod = getFeatures(677);
                var Anual = getPeriodName(param_Anual, featureSpecialPeriod);
                var periodname = Anual;

                context.output.iterator().each(function (key, value) {
                    var obj = JSON.parse(value);
                    if (obj["isError"] == "T") {
                        errores.push(JSON.stringify(obj["error"]));
                    } else {
                        ArrCustomer = obj.Customer;
                        monto_base = obj.Montobase;
                        MontoRet = obj.MontoRetenido;
                        porc = obj.Aliquota;
                        strReporte += Anual + ';' + ArrCustomer[1] + ';' + ArrCustomer[2] + ';' + ArrCustomer[0] + ';' + ArrCustomer[5] + ';' + ArrCustomer[3] + ';' + ArrCustomer[4] + ';' + ArrCustomer[6] + ';' + ArrCustomer[7] + ';' + monto_base + ';' + porc + ';' + MontoRet + '\r\n';

                    }
                    return true;
                });
                log.debug('strReporte', strReporte);

                //obtener nombre de subsidiaria
                var configpage = config.load({
                    type: config.Type.COMPANY_INFORMATION
                });

                if (feature_Subsi) {
                    companyname = ObtainNameSubsidiaria(param_Subsi);
                    companyname = _validateAccents(companyname);
                    companyruc = ObtainFederalIdSubsidiaria(param_Subsi);
                } else {
                    companyruc = configpage.getValue('employerid');
                    companyname = configpage.getValue('legalname');

                }

                companyruc = companyruc.replace(' ', '');
                companyruc = QuitaGuion(companyruc);

                if (strReporte == '') {
                    NoData();
                } else {
                    saveFile(strReporte, periodname);
                }
            } catch (err) {
                log.error('err', err);
                //libreria.sendMail(LMRY_script, ' [ getInputData ] ' + err);
            }
        }

        function getTaxResults(transactionID, lineUniqueKey) {
            var DbolStop = false;
            var intDMinReg = 0;
            var intDMaxReg = 1000;
            var ArrReturn = [];

            var savedsearch = search.create({
                type: "customrecord_lmry_br_transaction",
                filters: [
                    ["custrecord_lmry_br_transaction", "is", transactionID],
                    "AND",
                    ["custrecord_lmry_lineuniquekey", "equalto", lineUniqueKey],
                    "AND",
                    ["custrecord_lmry_br_type", "is", "ReteICA"]
                ],
                columns: [
                    search.createColumn({
                        name: "formulanumeric",
                        formula: "{custrecord_lmry_base_amount}",
                        label: "0. Base Amount"
                    }),
                    search.createColumn({
                        name: "formulanumeric",
                        formula: "{custrecord_lmry_br_total}",
                        label: "1. Imposto"
                    }),
                    search.createColumn({
                        name: "formulanumeric",
                        formula: "{custrecord_lmry_br_percent}",
                        label: "2. Percentage"
                    }),
                    search.createColumn({
                        name: "formulanumeric",
                        formula: "{custrecord_lmry_base_amount_local_currc}",
                        label: "3. Base Amount Local Currency"
                    }),
                    search.createColumn({
                        name: "formulanumeric",
                        formula: "{custrecord_lmry_amount_local_currency}",
                        label: "4. Impuesto Local Currency"
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "{custrecord_lmry_accounting_books}",
                        label: "5. TC's"
                    })
                ]
            });

            var searchresult = savedsearch.run();

            while (!DbolStop) {
                var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

                if (objResult != null) {

                    if (objResult.length != 1000) {
                        DbolStop = true;
                    }

                    var intLength = objResult.length;

                    for (var i = 0; i < intLength; i++) {
                        var columns = objResult[i].columns;
                        var arr = new Array();

                        //TC
                        var exchangeRate = exchange_rate(objResult[i].getValue(columns[5]));

                        // 0. Base Amount
                        var montoBase = objResult[i].getValue(columns[3]);
                        if (montoBase != null && montoBase != 0 && montoBase != "- None -") {
                            arr[0] = Number(montoBase);
                        } else {
                            arr[0] = objResult[i].getValue(columns[0]) * exchangeRate;

                        }
                        // 1. Retencion
                        var impuesto = objResult[i].getValue(columns[4]);
                        if (impuesto != null && impuesto != 0 && impuesto != "- None -") {
                            arr[1] = Number(impuesto);
                        } else {
                            arr[1] = objResult[i].getValue(columns[1]) * exchangeRate;
                        }

                        // 2. Percent
                        arr[2] = Number(objResult[i].getValue(columns[2])) * 10000;

                        ArrReturn.push(arr);
                    }

                    if (!DbolStop) {
                        intDMinReg = intDMaxReg;
                        intDMaxReg += 1000;
                    }

                } else {
                    DbolStop = true;
                }
            }

            return ArrReturn;
        }

        function getGlobalLabels() {
            var labels = {
                cabecera: {
                    en: 'VALIDITY' + ';' + 'DOCUMENT TYPE' + ';' + 'DOCUMENT NUMBER' + ';' + 'NAME OR BUSINESS NAME' + ';' + 'NOTIFICATION ADDRESS' + ';' + 'PHONE' + ';' + 'E-MAIL' + ';' + 'MUNICIPALITY CODE' + ';' + 'DPT CODE' + ';' + 'PAYMENT AMOUNT' + ';' + 'WITHHOLDING RATE APPLIED' + ';' + 'ANNUAL WITHHOLDING AMOUNT' + '\r\n',
                    es: 'VIGENCIA' + ';' + 'TIPO DE DOCUMENTO' + ';' + 'NUMERO DE DOCUMENTO' + ';' + 'NOMBRE O RAZON SOCIAL' + ';' + 'DIRECCION DE NOTIFICACION' + ';' + 'TELEFONO' + ';' + 'EMAIL' + ';' + 'CODIGO DE MUNICIPIO' + ';' + 'CODIGO DE DEPARTAMENTO' + ';' + 'MONTO PAGO' + ';' + 'TARIFA RETENCION APLICADA' + ';' + 'MONTO RETENCION ANUAL' + '\r\n',
                    pt: 'VALIDADE' + ';' + 'TIPO DO DOCUMENTO' + ';' + 'NUMERO DO DOCUMENTO' + ';' + 'NOME OU NOME DA EMPRESA' + ';' + 'ENDERECO DE NOTIFICACAO' + ';' + 'TELEFONE' + ';' + 'E-MAIL' + ';' + 'CODIGO DO MUNICIPIO' + ';' + 'CODIGO DO DEPARTAMENTO' + ';' + 'VALOR DO PAGAMENTO' + ';' + 'TAXA DE RETENCAO APLICADA' + ';' + 'VALOR DE RETENCAO ANUAL' + '\r\n'
                },
                nodata: {
                    en: 'There is no information for the selected criteria.',
                    es: 'No existe informacion para los criterios seleccionados.',
                    pt: 'nao ha informacoes para os criterios selecionados'
                },
                error: {
                    en: 'An unexpected error occurred while generating the report',
                    es: 'Ocurrio un error inesperado al generar el reporte',
                    pt: 'Ocorreu um erro inesperado ao gerar o relatorio'
                }
            }

            return labels;
        }

        function saveFile(strReporte, periodname) {
            var folderId = objContext.getParameter({
                name: 'custscript_lmry_file_cabinet_rg_co'
            });
            // Almacena en la carpeta de Archivos Generados
            if (folderId != '' && folderId != null) {
                // Extension del archivo
                if (param_head == 'T') {
                    var fileExt = '.csv';
                    var nameFile = NameFile(periodname) + fileExt;

                    // strCabecera = 'VIGENCIA' + ';' + 'TIPO DE DOCUMENTO' + ';' + 'NUMERO DE DOCUMENTO' + ';' + 'NOMBRE O RAZON SOCIAL' + ';' + 'DIRECCION DE NOTIFICACION' + ';' + 'TELEFONO' + ';' + 'EMAIL' + ';' + 'CODIGO DE MUNICIPIO' + ';' + 'CODIGO DE DEPARTAMENTO' + ';' + 'MONTO PAGO' + ';' + 'TARIFA RETENCION APLICADA' + ';' + 'MONTO RETENCION ANUAL';

                    var globalLabels = getGlobalLabels();
                    var titulo = globalLabels.cabecera[language];
                    
                    // Crea el archivo
                    var reportFile = fileModulo.create({
                        name: nameFile,
                        fileType: fileModulo.Type.CSV,
                        contents: titulo + strReporte,
                        encoding: fileModulo.Encoding.UTF8,
                        folder: folderId
                    });
                } else {
                    var fileExt = '.txt';
                    var nameFile = NameFile(periodname) + fileExt;

                    // Crea el archivo
                    var reportFile = fileModulo.create({
                        name: nameFile,
                        fileType: fileModulo.Type.PLAINTEXT,
                        contents: strReporte,
                        encoding: fileModulo.Encoding.UTF8,
                        folder: folderId
                    });
                }

                var idFile = reportFile.save();

                var idfile2 = fileModulo.load({
                    id: idFile
                }); // Trae URL de archivo generado

                // Obtenemo de las prefencias generales el URL de Netsuite (Produccion o Sandbox)
                var getURL = objContext.getParameter({
                    name: 'custscript_lmry_netsuite_location'
                });
                var urlfile = '';

                if (getURL != '' && getURL != '') {
                    urlfile += 'https://' + getURL;
                }

                urlfile += idfile2.url;

                log.debug({
                    title: 'url',
                    details: urlfile
                });

                //Genera registro personalizado como log
                var nombre = search.lookupFields({
                    type: "customrecord_lmry_co_features",
                    id: param_FeatID,
                    columns: ['name']
                });
                namereport = nombre.name;

                if (idFile) {
                    var usuarioTemp = runtime.getCurrentUser();
                    var id = usuarioTemp.id;
                    var employeename = search.lookupFields({
                        type: search.Type.EMPLOYEE,
                        id: id,
                        columns: ['firstname', 'lastname']
                    });
                    var usuario = employeename.firstname + ' ' + employeename.lastname;

                    if (false) {
                        var record = recordModulo.create({
                            type: 'customrecord_lmry_co_rpt_generator_log',
                        });

                        //Nombre de Reporte
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_rg_transaction',
                            value: 'CO - Art 4'
                        });

                        //Nombre de Subsidiaria
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_rg_subsidiary',
                            value: companyname
                        });

                        //Multibook
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_rg_multibook',
                            value: multibookName
                        });

                    } else {

                        var record = recordModulo.load({
                            type: 'customrecord_lmry_co_rpt_generator_log',
                            id: param_RecorID
                        });

                    }

                    //Nombre de Archivo
                    record.setValue({
                        fieldId: 'custrecord_lmry_co_rg_name',
                        value: nameFile
                    });
                    //Url de Archivo
                    record.setValue({
                        fieldId: 'custrecord_lmry_co_rg_url_file',
                        value: urlfile
                    });
                    //Periodo
                    record.setValue({
                        fieldId: 'custrecord_lmry_co_rg_postingperiod',
                        value: periodname
                    });
                    //Creado Por
                    record.setValue({
                        fieldId: 'custrecord_lmry_co_rg_employee',
                        value: usuario
                    });

                    var recordId = record.save();

                    // Envia mail de conformidad al usuario
                    libReport.sendConfirmUserEmail(namereport, 3, nameFile, language);
                }
            } else {
                log.debug("No se encontro folder");
            }
        }

        function redondear(number) {
            return Math.round(Number(number));
        }

        function getWHTJournal() {
            var intDMinReg = 0;
            var intDMaxReg = 1000;
            var DbolStop = false;
            var arrResult = new Array();

            var savedsearch = search.load({
                /*LatamReady - CO ART4 WHT Journal*/
                id: 'customsearch_lmry_co_art4_wht_journal'
            });

            if (feature_Subsi) {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [param_Subsi]
                });
                savedsearch.filters.push(subsidiaryFilter);
            }

            if (param_Anual != null && param_Anual != '') {
                var formulaPeriod = getFormulaPeriod(param_Anual, hasMulticalendarFeature);
                var periodFilter = search.createFilter({
                    name: 'formulatext',
                    formula: formulaPeriod,
                    operator: search.Operator.IS,
                    values: "1"
                });
                savedsearch.filters.push(periodFilter);
            }

            if (feature_Multi) {
                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [param_Multi]
                });
                savedsearch.filters.push(multibookFilter);
            }

            var searchResult = savedsearch.run();

            while (!DbolStop) {
                var objResult = searchResult.getRange(intDMinReg, intDMaxReg);
                //log.debug('tamaño de la busqueda', objResult.length);
                if (objResult != null) {

                    if (objResult.length != 1000) {
                        DbolStop = true;
                    }

                    for (var i = 0; i < objResult.length; i++) {
                        var columns = objResult[i].columns;
                        var arrAuxiliar = new Array();

                        for (var j = 0; j < columns.length; j++) {
                            arrAuxiliar[j] = objResult[i].getValue(columns[j]);
                        }
                        //LLenamos los valores en el arreglo
                        arrResult.push(arrAuxiliar);
                    }

                    if (!DbolStop) {
                        intDMinReg = intDMaxReg;
                        intDMaxReg += 1000;
                    }
                } else {
                    DbolStop = true;
                }
            }

            return arrResult;
        }

        function getWHTLines() {
            var intDMinReg = 0;
            var intDMaxReg = 1000;
            var DbolStop = false;
            //para la busqueda de transacciones
            var arrReturn = new Array();

            var savedsearch = search.load({
                /*LatamReady - CO Articulo 6 Line Level*/
                id: 'customsearch_lmry_co_art_6_line'
            });

            if (feature_Subsi) {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [param_Subsi]
                });
                savedsearch.filters.push(subsidiaryFilter);
            }

            if (param_Anual != null && param_Anual != '') {
                var formulaPeriod = getFormulaPeriod(param_Anual, hasMulticalendarFeature);
                var periodFilter = search.createFilter({
                    name: 'formulatext',
                    formula: formulaPeriod,
                    operator: search.Operator.IS,
                    values: "1"
                });
                savedsearch.filters.push(periodFilter);
            }
            if (hasJobsFeature && !hasAdvancedJobsFeature) {
                var customerColumn = search.createColumn({
                    name: 'formulanumeric',
                    formula: '{customermain.internalid}',
                    summary: 'GROUP'
                });
                savedsearch.columns.push(customerColumn);
            } else if ((!hasJobsFeature && !hasAdvancedJobsFeature) || (!hasJobsFeature && hasAdvancedJobsFeature) || (hasJobsFeature && hasAdvancedJobsFeature)) {
                var customerColumn = search.createColumn({
                    name: "formulanumeric",
                    formula: "CASE WHEN NVL({job.internalid},-1) = -1 THEN {customer.internalid} ELSE {job.customer.id} end",
                    summary: "GROUP"
                });
                savedsearch.columns.push(customerColumn);
            }

            var columnaMultibook = search.createColumn({
                name: 'formulatext',
                summary: 'Group',
                formula: "{custrecord_lmry_br_transaction.custrecord_lmry_accounting_books}",
                label: " Multibook"
            });
            savedsearch.columns.push(columnaMultibook);
            var searchResult = savedsearch.run();

            while (!DbolStop) {
                var objResult = searchResult.getRange(intDMinReg, intDMaxReg);
                if (objResult != null) {
                    if (objResult.length != 1000) {
                        DbolStop = true;
                    }

                    for (var i = 0; i < objResult.length; i++) {
                        var columns = objResult[i].columns;
                        var arrAuxiliar = new Array();
                        // 0. id Customer
                        if (objResult[i].getValue(columns[3]) != '' && objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -') {
                            arrAuxiliar[0] = objResult[i].getValue(columns[3]);
                        } else {
                            arrAuxiliar[0] = '';
                        }
                        /*TC MULTIBOOK*/
                        if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -') {
                            var exch_rate_nf = objResult[i].getValue(columns[4]);
                            exch_rate_nf = exchange_rate(exch_rate_nf);
                        } else {
                            exch_rate_nf = 1;
                        }
                        //1. monto base
                        arrAuxiliar[1] = objResult[i].getValue(columns[0]) * exch_rate_nf;
                        //2. tarifa retencion aplicada
                        arrAuxiliar[2] = (objResult[i].getValue(columns[1])) * 10000;
                        //3. monto retencion anual
                        arrAuxiliar[3] = objResult[i].getValue(columns[2]) * exch_rate_nf;

                        arrReturn.push(arrAuxiliar);
                    }
                    if (!DbolStop) {
                        intDMinReg = intDMaxReg;
                        intDMaxReg += 1000;
                    }
                } else {
                    DbolStop = true;
                }
            }

            return arrReturn;
        }

        function getWHTCabecera() {

            var arrReturn = new Array();
            intDMinReg = 0;
            intDMaxReg = 1000;
            DbolStop = false;

            var savedsearch_2 = search.load({
                /*LatamReady - CO Articulo 6 Main Level*/
                id: 'customsearch_lmry_co_art_6_main'
            });

            if (feature_Subsi) {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [param_Subsi]
                });
                savedsearch_2.filters.push(subsidiaryFilter);
            }

            if (param_Anual != null && param_Anual != '') {
                var formulaPeriod = getFormulaPeriod(param_Anual, hasMulticalendarFeature);
                var periodFilter = search.createFilter({
                    name: 'formulatext',
                    formula: formulaPeriod,
                    operator: search.Operator.IS,
                    values: "1"
                });
                savedsearch_2.filters.push(periodFilter);
            }
            //4
            if (hasJobsFeature && !hasAdvancedJobsFeature) {
                var customerColumn = search.createColumn({
                    name: 'formulanumeric',
                    formula: '{customermain.internalid}',
                    summary: 'GROUP'
                });
                savedsearch_2.columns.push(customerColumn);
            } else if ((!hasJobsFeature && !hasAdvancedJobsFeature) || (!hasJobsFeature && hasAdvancedJobsFeature) || (hasJobsFeature && hasAdvancedJobsFeature)) {
                var customerColumn = search.createColumn({
                    name: "formulanumeric",
                    formula: "CASE WHEN NVL({job.internalid},-1) = -1 THEN {customer.internalid} ELSE {job.customer.id} end",
                    summary: "GROUP"
                });
                savedsearch_2.columns.push(customerColumn);
            }

            if (feature_Multi) {
                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [param_Multi]
                });
                savedsearch_2.filters.push(multibookFilter);
                //5
                var customerColumn = search.createColumn({
                    name: "formulanumeric",
                    formula: "NVL({accountingTransaction.creditamount},0) - NVL({accountingTransaction.debitamount},0)",
                    summary: "SUM",
                    label: "Monto base multibook"
                });
                savedsearch_2.columns.push(customerColumn);
            }

            var searchResult = savedsearch_2.run();
            while (!DbolStop) {
                var objResult = searchResult.getRange(intDMinReg, intDMaxReg);
                if (objResult != null) {
                    if (objResult.length != 1000) {
                        DbolStop = true;
                    }

                    for (var i = 0; i < objResult.length; i++) {
                        var columns = objResult[i].columns;
                        var arrAuxiliar = new Array();
                        // 0. ID customer
                        if (objResult[i].getValue(columns[4]) != '' && objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -') {
                            arrAuxiliar[0] = objResult[i].getValue(columns[4]);
                        } else {
                            arrAuxiliar[0] = '';
                        }

                        //   //1.MULTIBOOK
                        //   if (objResult[i].getValue(columns[4])!=null && objResult[i].getValue(columns[4])!='- None -'){
                        //     var exch_rate_nf = objResult[i].getValue(columns[4]);
                        //     exch_rate_nf = exchange_rate(exch_rate_nf);
                        //   }else{
                        //     exch_rate_nf = 1;
                        //   }

                        //1. monto base
                        if (feature_Multi) {
                            if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '' && objResult[i].getValue(columns[5]) != '- None -' && objResult[i].getValue(columns[5]) != 'NaN') {
                                arrAuxiliar[1] = Number(objResult[i].getValue(columns[5]));
                            } else {
                                arrAuxiliar[1] = 0.00;
                            }
                        } else {
                            if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '' && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN') {
                                arrAuxiliar[1] = Number(objResult[i].getValue(columns[0]));
                            } else {
                                arrAuxiliar[1] = 0.00;
                            }
                        }
                        //3. tarifa retencion aplicada
                        arrAuxiliar[2] = (objResult[i].getValue(columns[1]));
                        //4. monto retencion anual
                        arrAuxiliar[3] = Number(objResult[i].getValue(columns[2]));

                        //  //id de retencion
                        //  log.error('no lo veo',objResult[i].getValue(columns[4]));
                        //  if (objResult[i].getValue(columns[11])!=null && objResult[i].getValue(columns[11])!='- None -'){
                        //     id_retencion = objResult[i].getValue(columns[11]);
                        //  }else{
                        //     id_retencion = '';
                        //  }
                        //  //6. monto base
                        //  if (id_retencion == 1) {
                        //    arrAuxiliar[5] = objResult[i].getValue(columns[6]);
                        //  }else if (id_retencion == 2) {
                        //    arrAuxiliar[5] = objResult[i].getValue(columns[8]);
                        //  }else if (id_retencion == 3) {
                        //    arrAuxiliar[5] = objResult[i].getValue(columns[7]);
                        //  }

                        //LLenamos los valores en el arreglo
                        arrReturn.push(arrAuxiliar);
                    }
                    if (!DbolStop) {
                        intDMinReg = intDMaxReg;
                        intDMaxReg += 1000;
                    }
                } else {
                    DbolStop = true;
                }
            }

            return arrReturn;
        }

        function NoData() {
            var globalLabels = getGlobalLabels();
            var mensajeNoData = globalLabels.nodata[language];
            var usuario = runtime.getCurrentUser();

            var periodenddate_temp = search.lookupFields({
                type: search.Type.ACCOUNTING_PERIOD,
                id: param_Anual,
                columns: ['periodname']
            });

            //Period StartDate
            var periodname = periodenddate_temp.periodname;

            var employee = search.lookupFields({
                type: search.Type.EMPLOYEE,
                id: usuario.id,
                columns: ['firstname', 'lastname']
            });
            var usuarioName = employee.firstname + ' ' + employee.lastname;

            var report = search.lookupFields({
                type: 'customrecord_lmry_co_features',
                id: param_FeatID,
                columns: ['name']
            });
            namereport = report.name;

            var generatorLog = recordModulo.load({
                type: 'customrecord_lmry_co_rpt_generator_log',
                id: param_RecorID
            });

            //Nombre de Archivo
            generatorLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_name',
                value: mensajeNoData
            });
            //Creado Por
            generatorLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_employee',
                value: usuarioName
            });
            generatorLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_transaction',
                value: namereport
            });

            var recordId = generatorLog.save();
        }


        function Remplaza_tildes(s) {
            var AccChars = "ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðñòóôõöùúûüýÿ°Ñ–—·";
            var RegChars = "SZszYAAAAAACEEEEIIIIDNOOOOOUUUUYaaaaaaceeeeiiiidnooooouuuuyyoN--.";
            s = s.toString();
            for (var c = 0; c < s.length; c++) {
                for (var special = 0; special < AccChars.length; special++) {
                    if (s.charAt(c) == AccChars.charAt(special)) {
                        s = s.substring(0, c) + RegChars.charAt(special) + s.substring(c + 1, s.length);
                    }
                }
            }
            return s;
        }

        function QuitaGuion(s) {
            var AccChars = "-./(),;_";
            var RegChars = "";
            s = String(s);
            for (var c = 0; c < s.length; c++) {
                for (var special = 0; special < AccChars.length; special++) {
                    if (s.charAt(c) == AccChars.charAt(special)) {
                        s = s.substring(0, c) + RegChars.charAt(special) + s.substring(c + 1, s.length);
                    }
                }
            }
            return s;
        }

        function Valida_caracteres_blanco(s) {
            var AccChars = "!“#$%&/()=\\-+/*ªº.,;ªº-_[]";
            var RegChars = "                           ";
            s = String(s);
            for (var c = 0; c < s.length; c++) {
                for (var special = 0; special < AccChars.length; special++) {
                    if (s.charAt(c) == AccChars.charAt(special)) {
                        s = s.substring(0, c) + RegChars.charAt(special) + s.substring(c + 1, s.length);
                    }
                }
            }
            return s;
        }

        function _validateAccents(cadena) {
            var acentos = {
                'Š': 'S', 'Ž': 'Z', 'š': 's', 'ž': 'z', 'Ÿ': 'Y', 'À': 'A', 'Á': 'A',
                'Â': 'A', 'Ã': 'A', 'Ä': 'A', 'Å': 'A', 'Ç': 'C', 'È': 'E', 'É': 'E',
                'Ê': 'E', 'Ë': 'E', 'Ì': 'I', 'Í': 'I', 'Î': 'I', 'Ï': 'I', 'Ð': 'D',
                'Ñ': 'N', 'Ò': 'O', 'Ó': 'O', 'Ô': 'O', 'Õ': 'O', 'Ö': 'O', 'Ù': 'U',
                'Ú': 'U', 'Û': 'U', 'Ü': 'U', 'Ý': 'Y', 'à': 'a', 'á': 'a', 'â': 'a',
                'ã': 'a', 'ä': 'a', 'å': 'a', 'ç': 'c', 'è': 'e', 'é': 'e', 'ê': 'e',
                'ë': 'e', 'ì': 'i', 'í': 'i', 'î': 'i', 'ï': 'i', 'ð': 'o', 'ñ': 'n',
                'ò': 'o', 'ó': 'o', 'ô': 'o', 'õ': 'o', 'ö': 'o', 'ù': 'u', 'ú': 'u',
                'û': 'u', 'ü': 'u', 'ý': 'y', 'ÿ': 'y', '&': 'y', '°': 'o', '–': '-',
                '—': '-', 'ª': 'a', 'º': 'o', '·': '.'
            };

            return cadena.split('').map(function (item) {
                return acentos[item] || item
            }).join('');
        }



        function validarAcentos(s) {
            var AccChars = "&°–—ªº·";
            var RegChars = "  --a .";

            s = s.toString();
            for (var c = 0; c < s.length; c++) {
                for (var special = 0; special < AccChars.length; special++) {
                    if (s.charAt(c) == AccChars.charAt(special)) {
                        s = s.substring(0, c) + RegChars.charAt(special) + s.substring(c + 1, s.length);
                    }
                }
            }
            return s;
        }

        function completar_cero(long, valor) {
            var length = ('' + valor).length;
            if (length <= long) {
                if (long != length) {
                    for (var i = length; i < long; i++) {
                        valor = '0' + valor;
                    }
                } else {
                    return valor;
                }
                return valor;
            } else {
                valor = ('' + valor).substring(0, long);
                return valor;
            }
        }

        function completar_espacio(long, valor) {
            if ((('' + valor).length) <= long) {
                if (long != ('' + valor).length) {
                    for (var i = (('' + valor).length); i < long; i++) {
                        valor = ' ' + valor;
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

        function ObtainNameSubsidiaria(subsidiary) {
            try {
                if (subsidiary != '' && subsidiary != null) {
                    var subsidyName = search.lookupFields({
                        type: search.Type.SUBSIDIARY,
                        id: subsidiary,
                        columns: ['legalname']
                    });
                    return subsidyName.legalname
                }
            } catch (err) {
                //libreria.sendMail(LMRY_script, ' [ ObtainNameSubsidiaria ] ' + err);
            }
            return '';
        }

        function ObtainFederalIdSubsidiaria(subsidiary) {
            try {
                if (subsidiary != '' && subsidiary != null) {
                    var federalId = search.lookupFields({
                        type: search.Type.SUBSIDIARY,
                        id: subsidiary,
                        columns: ['taxidnum']
                    });

                    return federalId.taxidnum
                }
            } catch (err) {
                //libreria.sendMail(LMRY_script, ' [ ObtainFederalIdSubsidiaria ] ' + err);
            }
            return '';
        }

        function NameFile(namePeriod) {
            var nameFile = 'ART6_';
            var periodname = '';
            if (param_Anual != '' && param_Anual != null) {
                periodname = namePeriod;
                log.debug('nombre del año', namePeriod);
            }


            var AAAA = periodname;

            if (feature_Multi || feature_Multi == 'T') {
                // if (paramContador != 0) {
                nameFile += companyruc + '_' + AAAA + '_' + param_Subsi + '_' + param_Multi;
                // } else {
                //     nameFile += companyruc +'_'+AAAA + '_' + param_Subsi + '_' + param_Multi;
                // }
            } else {
                //if (paramContador != 0) {
                nameFile += companyruc + '_' + AAAA + '_' + param_Subsi;
                // } else {
                //     nameFile += companyruc +'_'+AAAA + '_' + param_Subsi;
                // }
            }

            return nameFile;
        }

        function getCustomerData(idCustomer) {

            var customerEntity = search.lookupFields({
                type: search.Type.CUSTOMER,
                id: idCustomer,
                columns: ['isperson', 'companyname', 'firstname', 'lastname', 'vatregnumber', 'phone', 'email', "custentity_lmry_sunat_tipo_doc_id.custrecord_lmry_co_idtype_name"]
            });

            //log.debug('customerEntity',customerEntity);

            if (customerEntity != null && JSON.stringify(customerEntity) != '{}') {
                //1.tipo de documento
                var ide = customerEntity["custentity_lmry_sunat_tipo_doc_id.custrecord_lmry_co_idtype_name"];
                if (ide == 'CC' || ide == 'CE' || ide == 'TI' || ide == 'NIT' || ide == 'PA') {
                    ide = completar_espacio(3, ide);
                } else {
                    ide = '';
                }
                //2
                var vatregnumber = customerEntity.vatregnumber;
                if (vatregnumber == '' || vatregnumber == null || vatregnumber == 'NaN') {
                    vatregnumber = '';
                }
                var campo2 = QuitaGuion(vatregnumber).substring(0, 11);
                var campo3 = '';
                if (customerEntity.isperson) {
                    var first = customerEntity.firstname;
                    if (first == '' || first == null || first == 'NaN') {
                        first = '';
                    }

                    var last = customerEntity.lastname;
                    if (last == '' || last == null || last == 'NaN') {
                        last = '';
                    }

                    campo3 = first + ' ' + last;
                } else {
                    var raz = customerEntity.companyname;
                    if (raz == '' || raz == null || raz == 'NaN') {
                        raz = '';
                    }

                    campo3 = raz;
                }
                campo3 = Remplaza_tildes(campo3);
                campo3 = Valida_caracteres_blanco(campo3);
                campo3 = campo3.substring(0, 70);

                //5. telefono
                var campo5 = customerEntity.phone;
                if (campo5 != '' && campo5 != null && campo5 != 'NaN') {
                    campo5 = QuitaGuion(customerEntity.phone);
                    campo5 = campo5.substring(0, 10);
                } else {
                    campo5 = '';
                }
                //6. email
                var campo6 = customerEntity.email;
                if (campo6 == '' || campo6 == null || campo6 == 'NaN') {
                    campo6 = '';
                }

                var arrData = [campo3, ide, campo2, campo5, campo6];
                log.debug('arrData', arrData);
                return arrData;
            } else {
                return null;
            }

        }

        function getCustAddressData(id_customer) {
            var datos = search.create({
                type: "customer",
                filters: [
                    ["internalid", "anyof", id_customer],
                    "AND",
                    ["isdefaultbilling", "is", "T"]
                ],
                columns: [
                    search.createColumn({
                        name: "address1",
                        join: "billingAddress",
                        label: "Address 1"
                    }),
                    search.createColumn({
                        name: "address2",
                        join: "billingAddress",
                        label: "Address 2"
                    }),
                    search.createColumn({
                        name: 'formulatext',
                        formula: '{billingaddress.custrecord_lmry_addr_city_id}'
                    }),
                    search.createColumn({
                        name: "custrecord_lmry_addr_prov_id",
                        join: "billingAddress",
                        label: "Latam - Province ID"
                    })
                ]
            });

            var resultado = datos.run().getRange(0, 1000);
            if (resultado.length != 0) {
                var columns = resultado[0].columns;

                direccion = resultado[0].getValue(columns[0]) + ' ' + resultado[0].getValue(columns[1]);

                direccion = Valida_caracteres_blanco(direccion);
                direccion = Remplaza_tildes(direccion);
                direccion = direccion.substring(0, 70);
                municipio = resultado[0].getValue(columns[2]);
                departamento = resultado[0].getValue(columns[3]);
            } else {
                direccion = '';
                municipio = '';
                departamento = '';
            }

            return direccion + '|' + municipio + '|' + departamento;
        }

        function exchange_rate(exchangerate) {
            var auxiliar = ('' + exchangerate).split('&');
            var final = '';

            if (feature_Multi) {
                var id_libro = auxiliar[0].split('|');
                var exchange_rate = auxiliar[1].split('|');

                for (var i = 0; i < id_libro.length; i++) {
                    if (Number(id_libro[i]) == Number(param_Multi)) {
                        final = exchange_rate[i];
                        break;
                    } else {
                        final = exchange_rate[0];
                    }
                }
            } else {
                final = auxiliar[1];
            }
            return final;
        }

        function getFormulaPeriod(param_Anual, hasMulticalendarFeature) {
            featureSpecialPeriod = getFeatures(677);
            var arrPeriodsID = [];
            var formulPeriodFilters = ''

            if (featureSpecialPeriod == true || featureSpecialPeriod == 'T') {
                var searchPeriodSpecial = search.create({
                    type: "customrecord_lmry_special_accountperiod",
                    filters: [
                        ["isinactive", "is", "F"],
                        "AND", ["custrecord_lmry_anio_fisco", "is", param_Anual],
                        "AND", ["custrecord_lmry_adjustment", "is", "F"]
                    ],
                    columns: [
                        search.createColumn({
                            name: "custrecord_lmry_accounting_period"
                        })
                    ]
                });

                if (hasMulticalendarFeature == true || hasMulticalendarFeature == 'T') {
                    var subsiCalendar = search.lookupFields({
                        type: search.Type.SUBSIDIARY,
                        id: param_Subsi,
                        columns: ['fiscalcalendar']
                    });

                    var calendarSub = {
                        id: subsiCalendar.fiscalcalendar[0].value,
                        nombre: subsiCalendar.fiscalcalendar[0].text
                    }
                    calendarSub = JSON.stringify(calendarSub);

                    var fiscalCalendarFilter = search.createFilter({
                        name: 'custrecord_lmry_calendar',
                        operator: search.Operator.IS,
                        values: calendarSub
                    });
                    searchPeriodSpecial.filters.push(fiscalCalendarFilter);
                }

                var searchResult = searchPeriodSpecial.run().getRange(0, 100);

                if (searchResult.length != 0) {
                    for (i = 0; i < searchResult.length; i++) {
                        var columns = searchResult[i].columns;
                        arrPeriodsID.push(searchResult[i].getValue(columns[0]));
                    }
                }
            } else {
                var periodenddate_temp = search.lookupFields({
                    type: search.Type.ACCOUNTING_PERIOD,
                    id: param_Anual,
                    columns: ['enddate', 'periodname', 'startdate']
                });
                var periodstartdate = periodenddate_temp.startdate;
                var periodenddate = periodenddate_temp.enddate;

                var accountingperiodObj = search.create({
                    type: 'accountingperiod',
                    filters: [
                        ['isyear', 'is', 'F'],
                        'AND', ['isquarter', 'is', 'F'],
                        'AND', ['isadjust', 'is', 'F'],
                        'AND', ['startdate', 'onorafter', periodstartdate],
                        'AND', ['enddate', 'onorbefore', periodenddate]
                    ],
                    columns: [
                        search.createColumn({
                            name: "internalid",
                            //summary: "GROUP",
                            label: "Internal ID"
                        })
                    ]
                });
                if (hasMulticalendarFeature == true || hasMulticalendarFeature == 'T') {
                    var subsiCalendar = search.lookupFields({
                        type: search.Type.SUBSIDIARY,
                        id: param_Subsi,
                        columns: ['fiscalcalendar']
                    });

                    var calendarID = subsiCalendar.fiscalcalendar[0].value;

                    var fiscalCalendarFilter = search.createFilter({
                        name: 'fiscalcalendar',
                        operator: search.Operator.IS,
                        values: calendarID
                    });
                    accountingperiodObj.filters.push(fiscalCalendarFilter);
                }

                var searchResult = accountingperiodObj.run().getRange(0, 100);
                if (searchResult.length != 0) {
                    for (i = 0; i < searchResult.length; i++) {
                        var columns = searchResult[i].columns;
                        arrPeriodsID.push(searchResult[i].getValue(columns[0]));
                    }
                }
            }
            if (arrPeriodsID.length != 0) {
                formulPeriodFilters = generarStringFilterPostingPeriodAnual(arrPeriodsID);
            }

            return formulPeriodFilters;
        }

        function generarStringFilterPostingPeriodAnual(arrPeriodsID) {
            var cant = arrPeriodsID.length;
            var comSimpl = "'";
            var strinic = "CASE WHEN ({postingperiod.id}=" + comSimpl + arrPeriodsID[0] + comSimpl;
            var strAdicionales = "";
            var strfinal = ") THEN 1 ELSE 0 END";
            for (var i = 1; i < cant; i++) {
                strAdicionales += " or {postingperiod.id}=" + comSimpl + arrPeriodsID[i] + comSimpl;
            }
            var str = strinic + strAdicionales + strfinal;
            return str;
        }

        function getPeriodName(param_Anual, featureSpecialPeriod) {
            var anioName;
            if (featureSpecialPeriod == true || featureSpecialPeriod == 'T') {
                anioName = param_Anual
            } else {
                var periodenddate_temp = search.lookupFields({
                    type: search.Type.ACCOUNTING_PERIOD,
                    id: param_Anual,
                    columns: ['periodname']
                });
                //Period EndDate
                anioName = periodenddate_temp.periodname;
                var yearPeriod = anioName.split(' ');
                anioName = yearPeriod[1];
            }

            return anioName;
        }

        function getFeatures(idFeature) {
            var isActivate = false;
            var licenses = new Array();

            licenses = libReport.getLicenses(param_Subsi);
            isActivate = libReport.getAuthorization(idFeature, licenses);

            return isActivate;
        }

        return {
            getInputData: getInputData,
            map: map,
            reduce: reduce,
            summarize: summarize
        };

    });