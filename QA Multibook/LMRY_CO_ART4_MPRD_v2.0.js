/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_ART4_MPRD_v2.0.js                        ||
||                                                              ||
||  Version Date         Author        Remarks                  ||
||  2.0     Sep 04 2020  LatamReady    Use Script 2.0           ||
\= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.0
 * @NScriptType MapReduceScript
 * @NModuleScope Public
 */
define(['N/search', 'N/log', "N/config", 'N/file', 'N/format', 'N/runtime', "N/record", "./CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js", "/SuiteBundles/Bundle 37714/Latam_Library/LMRY_LibraryReport_LBRY_V2.js"],

    function (search, log, config, fileModulo, format, runtime, recordModulo, libreria, libReport) {

        var objContext = runtime.getCurrentScript();

        var LMRY_script = "LMRY_CO_ART4_MPRD_v2.0.js";

        //Parametros
        var param_RecorID = null;
        var param_Anual = null;
        var param_Multi = null;
        var param_Feature = null;
        var param_Subsi = null;
        var param_Titulo = null;

        // Features
        var feature_Subsi = null;
        var feature_Multi = null;
        var feature_MultiCalendar = null;
        var featureSpecialPeriod = null;

        var language = runtime.getCurrentScript().getParameter({
            name: 'LANGUAGE'
        }).substring(0, 2);

        if (language != "en" && language != "es" && language != "pt") {
            language = "es";
        }

        // Arrays Temporales
        var arrayCodsR = [];
        var arrayCodsSR = [];

        function getInputData() {
            try {
                getParameterAndFeatures();
                log.debug('empezo a correr la bsuqueda [Parametros]', 'Multib: ' + param_Multi + ', Subsi: ' + param_Subsi + ', Periodo: ' + param_Anual);
                log.debug('empezo a correr la bsuqueda [Features]', 'Subsi: ' + feature_Subsi + ', Multibook: ' + feature_Multi + ', MultiCalendar: ' + feature_MultiCalendar);
                var whtLines = getLinesWHT();
                log.debug('whtLines', whtLines);
                var whtTotal = getTotalWHT();
                log.debug('whtTotal', whtTotal);
                var whtJournalLines = getJournalWHT();
                log.debug('whtJournalLines', whtJournalLines);
                //recuerda retornar el arreglosgaaaaa
                var ArrReturn = whtLines.concat(whtTotal, whtJournalLines);
                //log.debug('valor del arreglo de retorno', ArrReturn);
                return ArrReturn;

            } catch (err) {
                log.error('error en Get Input Data', err);
                libReport.sendErrorEmail(err, LMRY_script, language);
                NoData(2);
                return [{
                    "isError": "T",
                    "error": err
                }];
            }
        }

        function map(context) {
            try {
                getParameterAndFeatures();
                var arrTemp = JSON.parse(context.value);
                var dataVendor = new Array();
                var montoBase = 0;
                var alicuota = 0;
                var montoRetencion = 0;
                var id_reduce;

                if (arrTemp[0] == 'Journal') {
                    //log.debug('entro a journal map');
                    var vendorEntity = getVendorData(arrTemp[3]);

                    if (vendorEntity != null) {
                        //log.debug('viene de journal');
                        var vendorDetailData = getVendorAddressData(arrTemp[3]);
                        var taxResults = getTaxResults(arrTemp[1], arrTemp[2]);

                        if (taxResults.length != 0) {
                            dataVendor = [vendorEntity[0], vendorEntity[4], vendorEntity[1], vendorEntity[2],
                            vendorDetailData[0], vendorDetailData[1], vendorDetailData[2], vendorEntity[3]
                            ];

                            montoBase = taxResults[0][0];
                            alicuota = taxResults[0][2];
                            montoRetencion = taxResults[0][1];

                            id_reduce = arrTemp[3] + '|' + alicuota; //ID VENDOR + ALIQUOTA

                            context.write({
                                key: id_reduce,
                                value: {
                                    Vendor: dataVendor,
                                    Montobase: montoBase,
                                    Aliquota: alicuota,
                                    MontoRetenido: montoRetencion
                                }
                            });

                        } else {
                            //log.debug('No hay taxresult en journal', vendorEntity);
                            //log.debug('Data sin tax results', arrTemp);
                            return false;
                        }
                    } else {
                        return false;
                    }

                } else {
                    //Retenciones por el total
                    if (arrTemp[8] == 'RetexTotal') {
                        //Datos Vendor
                        var datos = getVendorAddressData(arrTemp[3]);
                        var vendorEntity = getVendorData(arrTemp[3]);
                        if (feature_Multi) {
                            var datosRete = ObtenerDatosTransaccion(arrTemp[6], 2);
                        } else {
                            var datosRete = ObtenerDatosTransaccion(arrTemp[6], 1);
                        }

                        if (arrTemp[7] == 'VendCred') {
                            datosRete[0] = datosRete[0] * -1;
                            datosRete[1] = datosRete[1] * -1;
                        }

                        dataVendor = [arrTemp[0], vendorEntity[4], arrTemp[2], arrTemp[4], datos[0], datos[1], datos[2], arrTemp[5]];
                        montoBase = datosRete[0];
                        alicuota = datosRete[2];
                        montoRetencion = datosRete[1];

                        id_reduce = arrTemp[3] + '|' + datosRete[2]; //ID VENDOR + ALIQUOTA

                        context.write({
                            key: id_reduce,
                            value: {
                                Vendor: dataVendor,
                                Montobase: montoBase,
                                Aliquota: alicuota,
                                MontoRetenido: montoRetencion
                            }
                        });

                    } else {
                        //Retenciones por lineas
                        //log.debug('entro a retenciones por linea', arrTemp);
                        var datos = getVendorAddressData(arrTemp[3]);
                        var vendorEntity = getVendorData(arrTemp[3]);

                        var estadotransac = arrTemp[10].split('Reclasification').length > 1;
                        if (estadotransac == true) {
                            if (arrayCodsR.indexOf(arrTemp[9]) == -1) {
                                //log.debug('entro al map reclasificación por linea', estadotransac);
                                arrayCodsR.push(arrTemp[9]);
                                var datos_r = ObtenerDatosTaxResultRJL(arrTemp[7], arrTemp[9], 2);
                                datos_r.forEach(function (x) {
                                    var monto_base = x[0];
                                    var monto_total = x[1];
                                    var percentage = x[2];

                                    if (arrTemp[8] == 'VendCred') {
                                        monto_base = monto_base * -1;
                                        monto_total = monto_total * -1;
                                    }

                                    dataVendor = [arrTemp[0], vendorEntity[4], arrTemp[2], arrTemp[4], datos[0], datos[1], datos[2], arrTemp[5]];
                                    montoBase = monto_base;
                                    alicuota = percentage;
                                    montoRetencion = monto_total;

                                    id_reduce = arrTemp[3] + '|' + percentage; //ID VENDOR + ALIQUOTA

                                    context.write({
                                        key: id_reduce,
                                        value: {
                                            Vendor: dataVendor,
                                            Montobase: montoBase,
                                            Aliquota: alicuota,
                                            MontoRetenido: montoRetencion
                                        }
                                    });

                                });
                            }
                        } else {
                            if (arrayCodsSR.indexOf(arrTemp[7]) == -1) {
                                //log.debug('entro al map sin reclasificacion por linea', arrayCodsSR);
                                arrayCodsSR.push(arrTemp[7]);
                                var datos_r = ObtenerDatosTaxResultRJL(arrTemp[7], arrTemp[9], 1);
                                datos_r.forEach(function (x) {
                                    var monto_base = x[0];
                                    var monto_total = x[1];
                                    var percentage = x[2];

                                    if (arrTemp[8] == 'VendCred') {
                                        monto_base = monto_base * -1;
                                        monto_total = monto_total * -1;
                                    }

                                    dataVendor = [arrTemp[0], vendorEntity[4], arrTemp[2], arrTemp[4], datos[0], datos[1], datos[2], arrTemp[5]];
                                    montoBase = monto_base;
                                    alicuota = percentage;
                                    montoRetencion = monto_total;

                                    id_reduce = arrTemp[3] + '|' + percentage; //ID VENDOR + ALIQUOTA

                                    context.write({
                                        key: id_reduce,
                                        value: {
                                            Vendor: dataVendor,
                                            Montobase: montoBase,
                                            Aliquota: alicuota,
                                            MontoRetenido: montoRetencion
                                        }
                                    });

                                });
                            }
                        }

                        return true;
                    }
                }

            } catch (err) {
                log.error('error en Map', err);
                libReport.sendErrorEmail(err, LMRY_script, language);
                context.write({
                    key: context.key,
                    value: {
                        isError: "T",
                        error: err
                    }
                });
            }
        }

        function reduce(context) {
            try {
                var ArrVendor = new Array();
                var monto_B = 0;
                var monto_R = 0;
                var por = '';
                var arreglo = context.values;
                var tamaño = arreglo.length;
                for (var i = 0; i < tamaño; i++) {
                    var obj = JSON.parse(arreglo[i]);

                    ArrVendor = obj.Vendor;
                    monto_B += obj.Montobase;
                    monto_R += obj.MontoRetenido;
                    por = (Number(obj.Aliquota) * 10).toFixed(2);

                }
                monto_B = redondear(monto_B);
                monto_R = redondear(monto_R);

                if (monto_B != 0 && monto_R != 0) {
                    context.write({
                        key: context.key,
                        value: {
                            Vendor: ArrVendor,
                            Montobase: monto_B,
                            Aliquota: por,
                            MontoRetenido: monto_R
                        }
                    });
                }
            } catch (err) {
                log.error('error en Reduce', err);
                libReport.sendErrorEmail(err, LMRY_script, language);
                context.write({
                    key: context.key,
                    value: {
                        isError: "T",
                        error: err
                    }
                });
            }
        }

        function summarize(context) {
            try {
                getParameterAndFeatures();
                strReporte = '';
                featureSpecialPeriod = getFeatures(677);
                Anual = getPeriodName(param_Anual, featureSpecialPeriod)
                //para obtener el año de generacion de reporte
                var periodname = Anual;
                var errores = [];

                var salto = '\r\n';
                context.output.iterator().each(function (key, value) {
                    var obj = JSON.parse(value);
                    if (obj["isError"] == "T") {
                        errores.push(JSON.stringify(obj["error"]));
                    } else {
                        ArrVendor = obj.Vendor;
                        monto_base = obj.Montobase;
                        MontoRet = obj.MontoRetenido;
                        porc = obj.Aliquota;

                        if (param_Titulo == 'T' || param_Titulo == true) {
                            strReporte += Anual + ',' + ArrVendor[1] + ',' + ArrVendor[2] + ',' + ArrVendor[0] + ',' + ArrVendor[4] + ',' + ArrVendor[7] + ',' + ArrVendor[3] + ',' + ArrVendor[5] + ',' + ArrVendor[6] + ',' + monto_base + ',' + porc + ',' + MontoRet + salto;
                        } else {
                            strReporte += Anual + ';' + ArrVendor[1] + ';' + ArrVendor[2] + ';' + ArrVendor[0] + ';' + ArrVendor[4] + ';' + ArrVendor[7] + ';' + ArrVendor[3] + ';' + ArrVendor[5] + ';' + ArrVendor[6] + ';' + monto_base + ';' + porc + ';' + MontoRet + salto;
                        }
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
                    companyname = validarAcentos(companyname);
                    companyruc = ObtainFederalIdSubsidiaria(param_Subsi);
                } else {
                    companyruc = configpage.getValue('employerid');
                    companyname = configpage.getValue('legalname');
                }

                companyruc = companyruc.replace(' ', '');
                companyruc = ValidaGuion(companyruc);

                if (strReporte == '') {
                    NoData(1);
                    return true;
                }

                var folderId = objContext.getParameter({
                    name: 'custscript_lmry_file_cabinet_rg_co'
                });

                if (errores.length > 0) {
                    NoData(2);
                } else {
                    // Almacena en la carpeta de Archivos Generados
                    if (folderId != '' && folderId != null) {
                        // Extension del archivo
                        if (param_Titulo == 'T' || param_Titulo == true) {
                            var fileExt = '.csv';
                            var nameFile = getNameFile(companyruc, Anual) + fileExt;
                            var globalLabels = getGlobalLabels();
                            var titulo = globalLabels.cabecera[language];

                            strReporte = titulo + strReporte;
                            // Crea el archivo
                            var reportFile = fileModulo.create({
                                name: nameFile,
                                fileType: fileModulo.Type.CSV,
                                contents: strReporte,
                                encoding: fileModulo.Encoding.ISO_8859_1,
                                folder: folderId
                            });
                        } else {
                            var fileExt = '.txt';
                            var nameFile = getNameFile(companyruc, Anual) + fileExt;

                            // Crea el archivo
                            var reportFile = fileModulo.create({
                                name: nameFile,
                                fileType: fileModulo.Type.PLAINTEXT,
                                contents: strReporte,
                                encoding: fileModulo.Encoding.ISO_8859_1,
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
                            id: param_Feature,
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
                                log.debug('Carga Linea de LOG');
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
                    }

                    log.debug('paso de guardar el archivo', idFile);
                }
            } catch (err) {
                log.error('error en Summarize', err);
                libReport.sendErrorEmail(err, LMRY_script, language);
                NoData(2);
            }
        }

        function getParameterAndFeatures() {
            //Parametros
            param_RecorID = objContext.getParameter({
                name: 'custscript_lmry_co_art4_recordid'
            });
            param_Anual = objContext.getParameter({
                name: 'custscript_lmry_co_art4_periodo_anual'
            });
            param_Multi = objContext.getParameter({
                name: 'custscript_lmry_co_art4_multibook'
            });
            param_Feature = objContext.getParameter({
                name: 'custscript_lmry_co_art4_feature'
            });
            param_Subsi = objContext.getParameter({
                name: 'custscript_lmry_co_art4_subsidiaria'
            });
            param_Titulo = objContext.getParameter({
                name: 'custscript_lmry_co_art4_cabecera'
            });

            //************FEATURES********************
            feature_Subsi = runtime.isFeatureInEffect({
                feature: "SUBSIDIARIES"
            });

            feature_Multi = runtime.isFeatureInEffect({
                feature: "MULTIBOOK"
            });

            feature_MultiCalendar = runtime.isFeatureInEffect({
                feature: 'MULTIPLECALENDARS'
            });
        }

        function getGlobalLabels() {
            var labels = {
                cabecera: {
                    en: 'VALIDITY' + ',' + 'DOCUMENT TYPE' + ',' + 'DOCUMENT NUMBER' + ',' + 'NAME OR BUSINESS NAME' + ',' + 'NOTIFICATION ADDRESS' + ',' + 'PHONE' + ',' + 'E-MAIL' + ',' + 'MUNICIPALITY CODE' + ',' + 'DPT CODE' + ',' + 'BASE WITHHOLDING' + ',' + 'WITHHOLDING RATE APPLIED' + ',' + 'ANNUAL WITHHOLDING AMOUNT' + '\r\n',
                    es: 'VIGENCIA' + ',' + 'TIPO DOCUMENTO' + ',' + 'NUMERO DE DOCUMENTO' + ',' + 'NOMBRE O RAZON SOCIAL' + ',' + 'DIRECCION DE NOTIFICACION' + ',' + 'TELEFONO' + ',' + 'E-MAIL' + ',' + 'CODIGO MUNICIPIO' + ',' + 'CODIGO DEPTO' + ',' + 'BASE RETENCION' + ',' + 'TARIFA RETENCION APLICADA' + ',' + 'MONTO RETENCION ANUAL' + '\r\n',
                    pt: 'VALIDADE' + ',' + 'TIPO DOCUMENTO' + ',' + 'NUMERO DO DOCUMENTO' + ',' + 'NOME OU NOME DA EMPRESA' + ',' + 'ENDERECO DE NOTIFICACAO' + ',' + 'TELEFONE' + ',' + 'E-MAIL' + ',' + 'CODIGO DO MUNICIPIO' + ',' + 'CODIGO DO DEPARTAMENTO' + ',' + 'BASE DE RETENCAO' + ',' + 'TAXA DE RETENCAO APLICADA' + ',' + 'VALOR DE RETENCAO ANUAL' + '\r\n'
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

        function getFormulaPeriod(param_Anual, feature_MultiCalendar) {
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

                if (feature_MultiCalendar == true || feature_MultiCalendar == 'T') {
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
                if (feature_MultiCalendar == true || feature_MultiCalendar == 'T') {
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

            log.debug('[getFormulaPeriod] ID periods ', arrPeriodsID);

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

        function getStartAndEndDate(param_Anual) {
            featureSpecialPeriod = getFeatures(677);
            var arrAux = [];
            var startDate;
            var endDate;
            if (featureSpecialPeriod == true || featureSpecialPeriod == 'T') {
                //Period Start Date
                var startDateTemp = '01/01/' + param_Anual;
                //Period End Date
                var endDateTemp = '31/12/' + param_Anual;

                var periodIniTemp = startDateTemp.split("/");
                var newDateIni = new Date(periodIniTemp[2], periodIniTemp[1] - 1, periodIniTemp[0]);
                var periodIni = format.parse({
                    value: newDateIni,
                    type: format.Type.DATE
                });
                startDate = format.format({
                    value: periodIni,
                    type: format.Type.DATE
                });

                var periodFinTemp = endDateTemp.split("/");
                var newDateFin = new Date(periodFinTemp[2], periodFinTemp[1] - 1, periodFinTemp[0]);
                var periodFin = format.parse({
                    value: newDateFin,
                    type: format.Type.DATE
                });
                endDate = format.format({
                    value: periodFin,
                    type: format.Type.DATE
                });

            } else {
                var periodenddate_temp = search.lookupFields({
                    type: search.Type.ACCOUNTING_PERIOD,
                    id: param_Anual,
                    columns: ['startdate', 'enddate']
                });
                //Period Start y End Date
                startDate = periodenddate_temp.startdate;
                endDate = periodenddate_temp.enddate;
            }

            arrAux[0] = startDate;
            arrAux[1] = endDate;

            return arrAux;
        }

        function getFeatures(idFeature) {
            var isActivate = false;
            var licenses = new Array();

            licenses = libReport.getLicenses(param_Subsi);
            isActivate = libReport.getAuthorization(idFeature, licenses);

            return isActivate;
        }

        function getLinesWHT() {
            var intDMinReg = 0;
            var intDMaxReg = 1000;
            var DbolStop = false;
            //para la busqueda de transacciones
            var arrResult = new Array();

            var savedsearch = search.load({
                /*LatamReady - CO ART4 Transaccion*/
                id: 'customsearch_lmry_co_art4_transaccione_3'
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
                var formulaPeriod = getFormulaPeriod(param_Anual, feature_MultiCalendar)
                log.debug('Formula Periodos', formulaPeriod)
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

                //10. Monto Multibook
                var amountMultiB = search.createColumn({
                    name: "amount",
                    join: "accountingTransaction",
                    label: "10. Monto Multibook"
                });
                savedsearch.columns.push(amountMultiB);
            }

            //11. Memo Main
            var memoMain = search.createColumn({
                name: "formulatext",
                formula: "{memomain}",
                label: "memomain"
            });
            savedsearch.columns.push(memoMain);

            var searchResult = savedsearch.run();
            //para usarlos en los formatos
            var auxiliar = '';
            while (!DbolStop) {
                var objResult = searchResult.getRange(intDMinReg, intDMaxReg);
                log.debug('tamaño de la busqueda getLinesWHT', objResult.length);
                if (objResult != null) {
                    if (objResult.length != 1000) {
                        DbolStop = true;
                    }

                    for (var i = 0; i < objResult.length; i++) {
                        var columns = objResult[i].columns;
                        var arrAuxiliar = new Array();
                        // 0. nombre
                        if (objResult[i].getValue(columns[0]) != '' && objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -') {
                            arrAuxiliar[0] = objResult[i].getValue(columns[0]);
                            arrAuxiliar[0] = ValidarCaracteres_Especiales(arrAuxiliar[0]);
                            arrAuxiliar[0] = Valida_colombia(arrAuxiliar[0]);
                            arrAuxiliar[0] = arrAuxiliar[0].substring(0, 70);
                        } else {
                            arrAuxiliar[0] = '';
                        }
                        //1. tipo documento
                        if (objResult[i].getValue(columns[1]) != '' && objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -') {
                            arrAuxiliar[1] = objResult[i].getValue(columns[1]);
                        } else {
                            arrAuxiliar[1] = '';
                        }
                        //2.codigo
                        if (objResult[i].getValue(columns[2]) != '' && objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -') {
                            arrAuxiliar[2] = objResult[i].getValue(columns[2]);
                            arrAuxiliar[2] = Valida_Codigo(arrAuxiliar[2]);
                            arrAuxiliar[2] = arrAuxiliar[2].substring(0, 11);

                        } else {
                            arrAuxiliar[2] = '';
                        }
                        //3.id vendor
                        arrAuxiliar[3] = objResult[i].getValue(columns[3]);
                        //4.email
                        if (objResult[i].getValue(columns[4]) != '' && objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -') {
                            arrAuxiliar[4] = objResult[i].getValue(columns[4]);
                            arrAuxiliar[4] = arrAuxiliar[4].substring(0, 70);
                        } else {
                            arrAuxiliar[4] = '';
                        }
                        //5. telefono
                        if (objResult[i].getValue(columns[5]) != '' && objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -') {
                            arrAuxiliar[5] = objResult[i].getValue(columns[5]);
                            arrAuxiliar[5] = ValidaGuion(arrAuxiliar[5]);
                            arrAuxiliar[5] = arrAuxiliar[5].substring(0, 10)
                        } else {
                            arrAuxiliar[5] = '';
                        }
                        //6. monto retenido
                        if (feature_Multi) {
                            arrAuxiliar[6] = Math.abs(objResult[i].getValue(columns[10]));
                        } else {
                            arrAuxiliar[6] = Math.abs(objResult[i].getValue(columns[6]));
                        }
                        //7. transaccion origen
                        arrAuxiliar[7] = objResult[i].getValue(columns[7]);
                        //8. tipo de transaccion relacionada
                        arrAuxiliar[8] = objResult[i].getValue(columns[8]);
                        //9. internal id
                        arrAuxiliar[9] = objResult[i].getValue(columns[9]);
                        //10. memo main
                        if (feature_Multi) {
                            arrAuxiliar[10] = objResult[i].getValue(columns[11]);
                        } else {
                            arrAuxiliar[10] = objResult[i].getValue(columns[10]);
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

        function getTotalWHT() {
            var arrResult = [];

            var savedsearch_2 = search.load({
                /*LatamReady - CO ART4 Totales*/
                id: 'customsearch_lmry_co_art4_totales_2_2'
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
                var formulaPeriod = getFormulaPeriod(param_Anual, feature_MultiCalendar)
                var periodFilter = search.createFilter({
                    name: 'formulatext',
                    formula: formulaPeriod,
                    operator: search.Operator.IS,
                    values: "1"
                });
                savedsearch_2.filters.push(periodFilter);
            }

            if (feature_Multi) {
                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [param_Multi]
                });
                savedsearch_2.filters.push(multibookFilter);
            }

            var searchResult = savedsearch_2.run();
            intDMinReg = 0;
            intDMaxReg = 1000;
            DbolStop = false;

            while (!DbolStop) {
                var objResult = searchResult.getRange(intDMinReg, intDMaxReg);
                log.debug('tamaño de la busqueda getTotalWHT', objResult.length);
                if (objResult != null) {
                    if (objResult.length != 1000) {
                        DbolStop = true;
                    }

                    for (var i = 0; i < objResult.length; i++) {
                        var columns = objResult[i].columns;
                        var arrAuxiliar = new Array();
                        // 0. nombre
                        if (objResult[i].getValue(columns[0]) != '' && objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -') {
                            arrAuxiliar[0] = objResult[i].getValue(columns[0]);
                            arrAuxiliar[0] = ValidarCaracteres_Especiales(arrAuxiliar[0]);
                            arrAuxiliar[0] = Valida_colombia(arrAuxiliar[0]);
                            arrAuxiliar[0] = arrAuxiliar[0].substring(0, 70);
                        } else {
                            arrAuxiliar[0] = '';
                        }
                        //1. tipo documento
                        if (objResult[i].getValue(columns[1]) != '' && objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -') {
                            arrAuxiliar[1] = objResult[i].getValue(columns[1]);
                        } else {
                            arrAuxiliar[1] = '';
                        }
                        //2.codigo
                        if (objResult[i].getValue(columns[2]) != '' && objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -') {
                            arrAuxiliar[2] = objResult[i].getValue(columns[2]);
                            arrAuxiliar[2] = Valida_Codigo(arrAuxiliar[2]);
                            arrAuxiliar[2] = arrAuxiliar[2].substring(0, 11);

                        } else {
                            arrAuxiliar[2] = '';
                        }
                        //3.id vendor
                        arrAuxiliar[3] = objResult[i].getValue(columns[3]);
                        //4.email
                        if (objResult[i].getValue(columns[4]) != '' && objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -') {
                            arrAuxiliar[4] = objResult[i].getValue(columns[4]);
                            arrAuxiliar[4] = arrAuxiliar[4].substring(0, 70);
                        } else {
                            arrAuxiliar[4] = '';
                        }
                        //5. telefono
                        if (objResult[i].getValue(columns[5]) != '' && objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -') {
                            arrAuxiliar[5] = objResult[i].getValue(columns[5]);
                            arrAuxiliar[5] = ValidaGuion(arrAuxiliar[5]);
                            arrAuxiliar[5] = arrAuxiliar[5].substring(0, 10);
                        } else {
                            arrAuxiliar[5] = '';
                        }
                        //6. Transacción Origen
                        arrAuxiliar[6] = objResult[i].getValue(columns[6]);
                        //7. Tipo de transacción relacionada
                        arrAuxiliar[7] = objResult[i].getValue(columns[7]);
                        //8. Para saber que es RetexTotal
                        arrAuxiliar[8] = 'RetexTotal';
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

        function getJournalWHT() {
            var arrResult = [];

            var savedsearch = search.load({
                /*LatamReady - CO ART4 WHT Journal*/
                id: 'customsearch_lmry_co_art4_wht_journal'
            });

            var memorizedFilter = search.createFilter({
                name: 'memorized',
                operator: search.Operator.IS,
                values: "F"
            });
            savedsearch.filters.push(memorizedFilter);

            if (feature_Subsi) {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [param_Subsi]
                });
                savedsearch.filters.push(subsidiaryFilter);
            }

            if (param_Anual != null && param_Anual != '') {
                var formulaPeriod = getFormulaPeriod(param_Anual, feature_MultiCalendar)
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
            intDMinReg = 0;
            intDMaxReg = 1000;
            DbolStop = false;

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

        function NoData(type) {
            var globalLabels = getGlobalLabels();
            var usuarioTemp = runtime.getCurrentUser();
            var id = usuarioTemp.id;
            var employeename = search.lookupFields({
                type: search.Type.EMPLOYEE,
                id: id,
                columns: ['firstname', 'lastname']
            });
            var usuario = employeename.firstname + ' ' + employeename.lastname;

            if (type == 1) {
                var message = globalLabels.nodata[language];
            } else {
                var message = globalLabels.error[language];
            }

            var record = recordModulo.load({
                type: 'customrecord_lmry_co_rpt_generator_log',
                id: param_RecorID
            });

            //Nombre de Archivo
            record.setValue({
                fieldId: 'custrecord_lmry_co_rg_name',
                value: message
            });

            //Creado Por
            record.setValue({
                fieldId: 'custrecord_lmry_co_rg_employee',
                value: usuario
            });

            var recordId = record.save();
        }

        function redondear(number) {
            return Math.round(Number(number));
        }

        function ValidaGuion(s) {
            var AccChars = "+./-[] (),";
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

        function ValidarCaracteres_Especiales(s) {
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

        function Valida_colombia(s) {
            var AccChars = "!“#$%&/()=\\+/*ªº.,;ªº-+_?¿®©";
            var RegChars = "                             ";
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

        function Valida_Codigo(s) {
            var AccChars = "!“#$%&/()=\\+/*ªº.,;ªº-+_";
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

        function getNameFile(a, b) {

            if (feature_Multi) {
                var nameFile = 'ART4_' + a + '_' + b + '_' + param_Subsi + '_' + param_Multi
            } else {
                var nameFile = 'ART4_' + a + '_' + b + '_' + param_Subsi;
            }

            return nameFile;
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
                    "AND", ["custrecord_lmry_lineuniquekey", "equalto", lineUniqueKey],
                    "AND", ["custrecord_lmry_br_type", "is", "ReteICA"]
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

        function getVendorData(id_vendor) {
            var vendorData = new Array();

            var vendorEntity = search.lookupFields({
                type: search.Type.VENDOR,
                id: id_vendor,
                columns: ["isperson", "companyname", "firstname", "lastname", "vatregnumber", "email", "phone",
                    "custentity_lmry_sunat_tipo_doc_id.custrecord_lmry_co_idtype_name"
                ]
            });

            if (vendorEntity != null && JSON.stringify(vendorEntity) != '{}') {
                var razonSocial;
                if (vendorEntity.isperson) {
                    razonSocial = vendorEntity.firstname + ' ' + vendorEntity.lastname;
                } else {
                    razonSocial = vendorEntity.companyname;
                }
                razonSocial = ValidarCaracteres_Especiales(razonSocial);
                razonSocial = Valida_colombia(razonSocial);
                razonSocial = razonSocial.substring(0, 70);

                vendorData.push(razonSocial); //0

                var vatReg = vendorEntity.vatregnumber;
                if (vatReg != '' && vatReg != null && vatReg != '- None -') {
                    vatReg = Valida_Codigo(vatReg);
                    vatReg = vatReg.substring(0, 11);
                } else {
                    vatReg = '';
                }
                vendorData.push(vatReg); //1

                var email = vendorEntity.email;
                if (email != '' && email != null && email != '- None -') {
                    email = email.substring(0, 70);
                } else {
                    email = '';
                }
                vendorData.push(email); //2

                var phone = vendorEntity.phone;
                if (phone != '' && phone != null && phone != '- None -') {
                    phone = ValidaGuion(phone);
                    phone = phone.substring(0, 10)
                } else {
                    phone = '';
                }
                vendorData.push(phone); //3

                vendorData.push(vendorEntity["custentity_lmry_sunat_tipo_doc_id.custrecord_lmry_co_idtype_name"]); //4

                return vendorData;
            } else {
                //log.error('No existe vendor con este id:', id_vendor);
                return null;
            }
        }

        function getVendorAddressData(id_vendor) {
            var datos = search.create({
                type: "vendor",
                filters: [
                    ["internalid", "anyof", id_vendor],
                    "AND", ["isdefaultbilling", "is", "T"]
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
                        /* name: "custrecord_lmry_addr_city_id",
                        join: "billingAddress",
                        label: "Latam - City ID" */
                    }),
                    search.createColumn({
                        name: "custrecord_lmry_addr_prov_id",
                        join: "billingAddress",
                        label: "Latam - Province ID"
                    })
                ]
            });

            var resultado = datos.run().getRange(0, 1000);
            //log.debug('resultado vendor', resultado);
            var arrResult = new Array();

            if (resultado.length != 0) {
                var columns = resultado[0].columns;

                var direccion = resultado[0].getValue(columns[0]) + ' ' + resultado[0].getValue(columns[1]);
                direccion = ValidarCaracteres_Especiales(direccion);
                direccion = Valida_colombia(direccion);
                direccion = direccion.substring(0, 70);
                //0. Direccion
                arrResult.push(direccion);
                //1. municipio
                arrResult.push(resultado[0].getValue(columns[2]));
                //2. departamento
                arrResult.push(resultado[0].getValue(columns[3]));
            } else {
                arrResult = ['', '', ''];
            }

            return arrResult;
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

            final = Number(final.replace(' ', ''));
            return final;
        }

        function ObtenerDatosTransaccion(id_transaction, type) {
            if (type == 1) {
                var transactionSearchObj = search.create({
                    type: "transaction",
                    filters:
                        [
                            ["type", "anyof", "VendBill", "VendCred"],
                            "AND",
                            ["internalid", "anyof", id_transaction],
                            "AND",
                            ["mainline", "is", "T"],
                        ],
                    settings: [
                        search.createSetting({
                            name: 'consolidationtype',
                            value: 'NONE'
                        })
                    ],
                    columns:
                        [
                            search.createColumn({
                                name: "formulanumeric",
                                formula: "{custbody_lmry_co_reteica.custrecord_lmry_wht_salebase.id}",
                                label: "0. base id"
                            }),
                            search.createColumn({
                                name: "formulanumeric",
                                formula: "{taxtotal}",
                                label: "1 tax total"
                            }),
                            search.createColumn({
                                name: "formulanumeric",
                                formula: "{totalamount}",
                                label: "2. gross total"
                            }),
                            search.createColumn({
                                name: "formulanumeric",
                                formula: "{totalamount} - {taxtotal}",
                                label: "3. net total"
                            }),
                            search.createColumn({
                                name: "formulanumeric",
                                formula: "{custbody_lmry_co_reteica_amount}",
                                label: "4 monto de retencion"
                            }),
                            search.createColumn({
                                name: "formulanumeric",
                                formula: "{custbody_lmry_co_reteica.custrecord_lmry_wht_coderate}",
                                label: "5 Porcentaje"
                            }),
                        ]
                });

                var objResult = transactionSearchObj.run().getRange(0, 20);
                var auxArray = new Array();
                var columns = objResult[0].columns;

                // monto base de la retencion
                if (objResult[0].getValue(columns[0]) == 1) {
                    auxArray[0] = Math.abs(objResult[0].getValue(columns[2]));
                } else if (objResult[0].getValue(columns[0]) == 2) {
                    auxArray[0] = Math.abs(objResult[0].getValue(columns[3]));
                } else if (objResult[0].getValue(columns[0]) == 3) {
                    auxArray[0] = Math.abs(objResult[0].getValue(columns[1]));
                } else {
                    auxArray[0] = Math.abs(objResult[0].getValue(columns[3]));
                }
                //monto de retencion
                auxArray[1] = Math.abs(objResult[0].getValue(columns[4]));
                //Porcentaje
                auxArray[2] = objResult[0].getValue(columns[5]);
            } else {
                var transactionSearchObj = search.create({
                    type: "transaction",
                    filters:
                        [
                            ["type", "anyof", "VendBill", "VendCred"],
                            "AND",
                            ["internalid", "anyof", id_transaction],
                            "AND",
                            ["mainline", "is", "F"],
                            "AND",
                            ["cogs", "is", "F"]
                        ],
                    settings: [
                        search.createSetting({
                            name: 'consolidationtype',
                            value: 'NONE'
                        })
                    ],
                    columns:
                        [
                            search.createColumn({
                                name: "formulanumeric",
                                summary: "GROUP",
                                formula: "{custbody_lmry_co_reteica.custrecord_lmry_wht_salebase.id}",
                                label: "ID SaleBase"
                            }),
                            search.createColumn({
                                name: "formulanumeric",
                                summary: "SUM",
                                formula: "CASE WHEN {taxline} = 'true' or {taxline} = 'T' THEN 0 ELSE NVL({accountingtransaction.debitamount},0)-NVL({accountingtransaction.creditamount},0) END",
                                label: "Subtotal"
                            }),
                            search.createColumn({
                                name: "formulanumeric",
                                summary: "SUM",
                                formula: "CASE WHEN {taxline} = 'true' or {taxline} = 'T' THEN NVL({accountingtransaction.debitamount},0)-NVL({accountingtransaction.creditamount},0) ELSE 0 END",
                                label: "Tax Total"
                            }),
                            search.createColumn({
                                name: "formulanumeric",
                                summary: "GROUP",
                                formula: "{custbody_lmry_co_reteica_amount}",
                                label: "Retencion"
                            }), search.createColumn({
                                name: "formulanumeric",
                                summary: "GROUP",
                                formula: "{custbody_lmry_co_reteica.custrecord_lmry_wht_coderate}",
                                label: "Porcentaje"
                            })
                        ]
                });

                if (feature_Multi) {
                    var multibookFilter = search.createFilter({
                        name: 'accountingbook',
                        join: 'accountingtransaction',
                        operator: search.Operator.IS,
                        values: [param_Multi]
                    });
                    transactionSearchObj.filters.push(multibookFilter);
                }

                var objResult = transactionSearchObj.run().getRange(0, 20);
                var auxArray = new Array();
                var columns = objResult[0].columns;

                // monto base de la retencion
                if (objResult[0].getValue(columns[0]) == 1) {
                    auxArray[0] = Number(Math.abs(objResult[0].getValue(columns[1]))) + Number(Math.abs(objResult[0].getValue(columns[2])));
                } else if (objResult[0].getValue(columns[0]) == 2) {
                    auxArray[0] = Math.abs(objResult[0].getValue(columns[1]));
                } else if (objResult[0].getValue(columns[0]) == 3) {
                    auxArray[0] = Math.abs(objResult[0].getValue(columns[2]));
                } else {
                    auxArray[0] = Math.abs(objResult[0].getValue(columns[1]));
                }
                //monto de retencion
                auxArray[1] = Math.abs(objResult[0].getValue(columns[3]));
                //Porcentaje
                auxArray[2] = objResult[0].getValue(columns[4]);
            }

            return auxArray;
        }

        function ObtenerDatosTaxResultRJL(id_transaction, id_journal, type) {
            var arrResult = [];
            var arrayIDs = ObtenerDatosTaxResultReclasification(id_transaction, id_journal, type);
            //log.debug('el arrayIds es: ', arrayIDs);

            var datosTaxResultRJL = search.create({
                type: "customrecord_lmry_br_transaction",
                filters:
                    [
                        ["formulatext: CASE WHEN NVL({custrecord_lmry_br_type},'') = 'ReteICA' THEN 1 ELSE 0 END", "is", "1"],
                        "AND",
                        ["custrecord_lmry_br_transaction.mainline", "is", "T"],
                        "AND",
                        ["formulatext: CASE WHEN ({custrecord_lmry_ntax.custrecord_lmry_ntax_gen_transaction.id}='1' OR {custrecord_lmry_ccl.custrecord_lmry_ccl_gen_transaction.id}='1') THEN 1 ELSE 0 END", "is", "0"],
                        "AND",
                        ["custrecord_lmry_br_transaction.internalid", "anyof", id_transaction]
                    ],
                columns:
                    [
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "NVL({custrecord_lmry_base_amount_local_currc},0)",
                            label: "0. Base Amount Local Currency"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "NVL({custrecord_lmry_amount_local_currency},0)",
                            label: "1. Amount Local Currency"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "NVL({custrecord_lmry_br_percent},0)",
                            label: "2. Porcentaje"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "{custrecord_lmry_accounting_books}",
                            label: "3. Multibook"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "NVL({custrecord_lmry_base_amount},0)",
                            label: "4. Base Amount"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "NVL({custrecord_lmry_br_total},0)",
                            label: "5. Amount"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "{internalid}",
                            label: "6. Internal ID"
                        })
                    ]
            });
            var pagedData = datosTaxResultRJL.runPaged({
                pageSize: 1000
            });

            var page, columns;

            pagedData.pageRanges.forEach(function (pageRange) {
                page = pagedData.fetch({
                    index: pageRange.index
                });

                page.data.forEach(function (result) {
                    columns = result.columns;
                    var arrAux = [];

                    if (type == 1) {
                        if (arrayIDs.length != 0) {
                            if (arrayIDs.indexOf(Number(result.getValue(columns[6]))) == -1) {
                                if (result.getValue(columns[0]) != 0 && result.getValue(columns[1]) != 0) {
                                    arrAux[0] = Math.abs(result.getValue(columns[0]));
                                    arrAux[1] = Math.abs(result.getValue(columns[1]));
                                    arrAux[2] = result.getValue(columns[2]) * 10000;
                                } else {
                                    if (result.getValue(columns[3]) != null && result.getValue(columns[3]) != '- None -') {
                                        var exchangeRate = result.getValue(columns[3]);
                                        exchangeRate = exchange_rate(exchangeRate);
                                    } else {
                                        var exchangeRate = 1;
                                    }

                                    arrAux[0] = Math.abs(result.getValue(columns[4])) * exchangeRate;
                                    arrAux[1] = Math.abs(result.getValue(columns[5])) * exchangeRate;
                                    arrAux[2] = result.getValue(columns[2]) * 10000;
                                }

                                arrResult.push(arrAux);
                            }
                        } else {
                            if (result.getValue(columns[0]) != 0 && result.getValue(columns[1]) != 0) {
                                arrAux[0] = Math.abs(result.getValue(columns[0]));
                                arrAux[1] = Math.abs(result.getValue(columns[1]));
                                arrAux[2] = result.getValue(columns[2]) * 10000;
                            } else {
                                if (result.getValue(columns[3]) != null && result.getValue(columns[3]) != '- None -') {
                                    var exchangeRate = result.getValue(columns[3]);
                                    exchangeRate = exchange_rate(exchangeRate);
                                } else {
                                    var exchangeRate = 1;
                                }

                                arrAux[0] = Math.abs(result.getValue(columns[4])) * exchangeRate;
                                arrAux[1] = Math.abs(result.getValue(columns[5])) * exchangeRate;
                                arrAux[2] = result.getValue(columns[2]) * 10000;
                            }

                            arrResult.push(arrAux);
                        }
                    } else {
                        if (arrayIDs.length != 0) {
                            if (arrayIDs.indexOf(Number(result.getValue(columns[6]))) != -1) {
                                if (result.getValue(columns[0]) != 0 && result.getValue(columns[1]) != 0) {
                                    arrAux[0] = Math.abs(result.getValue(columns[0]));
                                    arrAux[1] = Math.abs(result.getValue(columns[1]));
                                    arrAux[2] = result.getValue(columns[2]) * 10000;
                                } else {
                                    if (result.getValue(columns[3]) != null && result.getValue(columns[3]) != '- None -') {
                                        var exchangeRate = result.getValue(columns[3]);
                                        exchangeRate = exchange_rate(exchangeRate);
                                    } else {
                                        var exchangeRate = 1;
                                    }

                                    arrAux[0] = Math.abs(result.getValue(columns[4])) * exchangeRate;
                                    arrAux[1] = Math.abs(result.getValue(columns[5])) * exchangeRate;
                                    arrAux[2] = result.getValue(columns[2]) * 10000;
                                }

                                arrResult.push(arrAux);
                            }
                        }
                    }
                });

            });

            //log.debug('el arrResult TaxResults es: ', arrResult);

            return arrResult;
        }

        function ObtenerDatosTaxResultReclasification(id_transaccion_Ref, id_journal, type) {
            var periodData = getStartAndEndDate(param_Anual);
            var periodStartDate = periodData[0];
            var periodEndDate = periodData[1];
            var arrayIDs = [];

            var searchTaxResultReclasification = search.create({
                type: "customrecord_lmry_co_wht_reclasification",
                filters:
                    [],
                columns:
                    [
                        search.createColumn({
                            name: "custrecord_co_reclasification_return",
                        }),

                    ]
            });

            if (feature_Subsi) {
                var subsidiaryFilter = search.createFilter({
                    name: 'custrecord_co_reclasification_subsi',
                    operator: search.Operator.IS,
                    values: [param_Subsi]
                });
                searchTaxResultReclasification.filters.push(subsidiaryFilter);
            }

            if (type == 2) {

                if (param_Anual) {
                    var fechInicioFilter = search.createFilter({
                        name: 'custrecord_co_reclasification_condate',
                        operator: search.Operator.ONORAFTER,
                        values: [periodStartDate]
                    });
                    searchTaxResultReclasification.filters.push(fechInicioFilter);
                    var fechFinFilter = search.createFilter({
                        name: 'custrecord_co_reclasification_condate',
                        operator: search.Operator.ONORBEFORE,
                        values: [periodEndDate]
                    });
                    searchTaxResultReclasification.filters.push(fechFinFilter);
                }
            }

            var pagedData = searchTaxResultReclasification.runPaged({
                pageSize: 1000
            });

            var page, columns;

            pagedData.pageRanges.forEach(function (pageRange) {
                page = pagedData.fetch({
                    index: pageRange.index
                });

                page.data.forEach(function (result) {
                    columns = result.columns;
                    if (result.getValue(columns[0]) != '' && result.getValue(columns[0]) != null && result.getValue(columns[0]) != 0) {
                        var datosTemp = JSON.parse(result.getValue(columns[0]));
                        if (datosTemp[id_transaccion_Ref] != undefined) {
                            for (i = 0; i < datosTemp[id_transaccion_Ref].length; i++) {
                                if (type == 1) {
                                    arrayIDs.push(datosTemp[id_transaccion_Ref][i].taxResult);
                                } else {
                                    if (datosTemp[id_transaccion_Ref][i].retention == id_journal) {
                                        arrayIDs.push(datosTemp[id_transaccion_Ref][i].taxResult);
                                    }
                                }
                            }
                        }
                    }
                });

            });

            //log.debug('el arrayIDs es: ', arrayIDs);

            return arrayIDs;
        }

        return {
            getInputData: getInputData,
            map: map,
            reduce: reduce,
            summarize: summarize
        };

    });