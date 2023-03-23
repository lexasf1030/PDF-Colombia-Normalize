/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_ReporteMagAnual1006_MPRD.js              ||
||                                                              ||
||  Version Date         Author        Remarks                  ||
||  2.0     Jul 07 2020  LatamReady    Use Script 2.0           ||
\= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.0
 * @NScriptType MapReduceScript
 * @NModuleScope Public
 */
define(['N/record', 'N/runtime', 'N/file', 'N/email', 'N/search', 'N/encode', 'N/currency',
        'N/format', 'N/log', 'N/config', 'N/xml', 'N/task', './CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js'
    ],

    function(record, runtime, file, email, search, encode, currency, format, log, config, xml, task, libreria) {
        var objContext = runtime.getCurrentScript();

        // Nombre del Reporte
        var reportName = '"Reporte de Medios Magneticos: Formulario 1006 v8.1"';
        var LMRY_script = 'LMRY_CO_ReporteMagAnual1006v8.1_SCHDL_V2.0.js';
        var DbolStop = false;

        var paramSubsidiaria;
        var paramMultibook;
        var paramPeriodo;
        var paramIdLog;
        var paramBucle;
        var paramCont;
        var paramIdReport;
        var paramIdFeatureByVersion;
        var paramConcepto;
        var IMPUESTOGENERADO;
        var existeCuantias;

        var isMultibookFeature;
        var isSubsidiariaFeature;
        var isfeatJobs;
        var isAdvanceJobsFeature;

        var FILE_SIZE = 7700000; // 7700000
        var CANT_REGISTROS = 1000; //1000
        var MAX_CANT_REGISTROS = 5000; //5000
        var CUANTIA_MINIMA = 500000;

        var companyName;
        var companyRuc;
        var cantregistros = 0;
        var valorTotal = 0;
        var imprimirenExcelXml = false;


        var existeCuantias = 0;
        var periodStartDate;
        var periodEndDate;

        var periodName;
        var multibookName;
        var numeroEnvio;

        var currenciesJson = {};
        var arrJson = {};
        var ArrLibroMag = [];
        var strExcelVentasXPagar = '';
        var strXmlVentasXPagar = '';
        var generarXml = false;

        //Lenguaje

        var language = runtime.getCurrentScript().getParameter("LANGUAGE").substring(0, 2);
        var GLOBAL_LABELS = {};


        function getInputData() {
            try {
                var arrJournal = new Array();
                var arrInvoice = new Array();
                var arrBillCre = new Array();

                ObtenerParametrosYFeatures();
                ObtenerDatosSubsidiaria();

                if (isSubsidiariaFeature && isMultibookFeature) {
                    ObtenerCurrencies();
                    ObtenerCuantiaMenor();
                }
                log.error('CO LEAST AMOUNT', CUANTIA_MINIMA);

                arrJournal = ObtenerJournals();

                arrJourRef = ObtenerJournalsReferences();

                arrInvoice = ObtenerTransaction('Inv');

                arrBillCre = ObtenerTransaction('BCr');


                UnirArrays(arrJournal, arrJourRef, arrInvoice, arrBillCre);

                if (ArrLibroMag.length != 0) {
                    log.debug('ArrLibroMagqq', ArrLibroMag);
                    return ArrLibroMag;
                } else {
                    NoData();
                }

            } catch (error) {
                log.error('Error de getInputData', error);
            }
        }


        function map(context) {
            try {
                var arrTemp = JSON.parse(context.value);

                if (arrTemp["isError"] == "T") {
                    context.write({
                        key: context.key,
                        value: arrTemp
                    });
                } else {
                    var vectorMapTransacciones = arrTemp;
                    context.write({
                        key: context.key,
                        value: {
                            stringTransacciones: vectorMapTransacciones
                        }
                    });

                }
            } catch (error) {
                log.error('Error de Map', error);
            }
        }

        function summarize(context) {
            try {
                ObtenerParametrosYFeatures();
                ObtenerDatosSubsidiaria();
                var arrAuxiliar = new Array();
                var ArrLibroMag = new Array();
                var banderitaSumm = false;
                var infoTxt = '';
                var contarRegistros = 0;

                context.output.iterator().each(function(key, value) {
                    var obj = JSON.parse(value);

                    if (obj["isError"] == "T") {
                        errores.push(JSON.stringify(obj["error"]));
                    } else {
                        var arrTransaction = obj.stringTransacciones;
                        if (arrTransaction[0] == 'Auxiliar') {
                            log.debug('Banderita del Summ', arrTransaction[1]);
                            banderitaSumm = true;
                            existeCuantias = 1;
                            valorTotal = arrTransaction[2];
                            IMPUESTOGENERADO = arrTransaction[3];
                        } else {
                            arrAuxiliar.push(arrTransaction);
                        }
                    }
                    return true;
                });
                log.debug('mirame', arrAuxiliar);
                for (var i = 0; i < arrAuxiliar.length; i++) {
                    infoTxt = infoTxt + arrAuxiliar[i][0] + arrAuxiliar[i][1] + arrAuxiliar[i][2] + arrAuxiliar[i][3] + arrAuxiliar[i][4] + arrAuxiliar[i][5] + arrAuxiliar[i][6] + arrAuxiliar[i][7] + arrAuxiliar[i][8] + arrAuxiliar[i][9] + '\r\n';
                    ArrLibroMag.push(arrAuxiliar[i]);
                    var string_size_in_bytes = lengthInUtf8Bytes(infoTxt);

                    if (objContext.getRemainingUsage() <= 500 || string_size_in_bytes >= FILE_SIZE || contarRegistros >= MAX_CANT_REGISTROS) {
                        //paramCont = Number(paramCont) + 1;
                        paramBucle = Number(paramBucle) + 1;
                        numeroEnvio = obtenerNumeroEnvio();
                        if (ArrLibroMag.length > 0) {
                            GLOBAL_LABELS = getGlobalLabels();
                            paramCont = Number(paramCont) + 1;
                            GenerarExcel(ArrLibroMag);
                            generarXml = true;
                            GenerarXml(ArrLibroMag);
                        }
                    }
                    contarRegistros++;
                }

                if (banderitaSumm) {
                    infoTxt = infoTxt + '43' + '222222222' + '0' + '' + 'CUANTIAS MENORES' + IMPUESTOGENERADO;
                    imprimirenExcelXml = true;
                    valorTotal = valorTotal + 43;
                    //cantregistros = ArrLibroMag.length + 1;
                }
                log.debug('mirame 3', ArrLibroMag);
                if (ArrLibroMag.length || infoTxt != '') {
                    numeroEnvio = obtenerNumeroEnvio();
                    log.error('ArrLibroMag', ArrLibroMag);
                    log.error('infoTxt', infoTxt);
                    GLOBAL_LABELS = getGlobalLabels();
                    GenerarExcel(ArrLibroMag);
                    generarXml = true;
                    GenerarXml(ArrLibroMag);
                } else {
                    if (paramCont == 0) {
                        NoData();
                    }
                }


            } catch (error) {
                log.error('Error de SUMMARIZE', error);
            }
        }

        function getGlobalLabels() {
            var labels = {
                "titulo": {
                    "es": 'FORMULARIO 1006: IMPUESTO A LAS VENTAS POR PAGAR (GENERADO) E IMPUESTO AL CONSUMO',
                    "pt": 'FORMULARIO 1006: IMPOSTO DE VENDAS A PAGAR (GERADO) E IMPOSTO AO CONSUMIDOR',
                    "en": 'FORM 1006: SALES TAX PAYABLE (GENERATED) AND CONSUMER TAX'
                },
                "razonSocial": {
                    "es": 'Razon Social',
                    "pt": 'Razao social',
                    "en": 'Company name'
                },
                "taxNumber": {
                    "es": 'Numero de Identificacion Tributaria',
                    "pt": 'Numero de Identificacao Fiscal',
                    "en": 'Tax Number'
                },
                "al": {
                    "es": 'al',
                    "pt": 'ao',
                    "en": 'to the'
                },
                "periodo": {
                    "es": 'Periodo',
                    "pt": 'Periodo',
                    "en": 'Period'
                },
                "primerApellido": {
                    "es": '1er Apellido',
                    "pt": '1º Sobrenome',
                    "en": '1st Last Name'
                },
                "segApellido": {
                    "es": '2do Apellido',
                    "pt": '2º Sobrenome',
                    "en": '2nd Last Name'
                },
                "primerNombre": {
                    "es": '1er Nombre',
                    "pt": '1º nome',
                    "en": '1st Name'
                },
                "segNombre": {
                    "es": '2do Nombre',
                    "pt": '2º nome',
                    "en": '2nd Name'
                },
                "ImpGenerado": {
                    "es": 'Impuesto Generado',
                    "pt": 'Imposto Gerado',
                    "en": 'Tax Generated'
                },
                "IVArecuperadoDev": {
                    "es": 'IVA Recuperado Dev.',
                    "pt": 'IVA recuperado Dev.',
                    "en": 'IVA Recovered Dev.'
                },
                "Impuestoalconsumo": {
                    "es": 'Impuesto al consumo',
                    "pt": 'Taxa de consumo',
                    "en": 'Consumption tax'
                },
                'noData': {
                    "es": 'No existe informacion para los criterios seleccionados',
                    "pt": 'Não há informações para os critérios selecionados',
                    "en": 'There is no information for the selected criteria'
                },
                "origin": {
                  "es": "Origen",
                  "en": "Origin",
                  "pt": "Origem"
                },
                "date": {
                  "es": "Fecha",
                  "en": "Date",
                  "pt": "Data"
                },
                "time": {
                  "es": "Hora",
                  "en": "Time",
                  "pt": "Hora"
                }
            }

            return labels;
        }

        function UnirArrays(arrJour, arrJourR, arrInv, arrBill) {
            var arrJournal = arrJour;
            var arrJourRef = arrJourR;
            var arrInvoice = arrInv;
            var arrBillCre = arrBill;
            var arrAuxiliarFin = [];
            var arrAuxiliIva = [];
            var impuesto = 0;
            var valorTotal = 0;

            for (var i = 0; i < arrJournal.length; i++) {
                var banderita = false;
                for (var j = 0; j < arrAuxiliarFin.length; j++) {
                    if (arrJournal[i][0] == arrAuxiliarFin[j][0] &&
                        arrJournal[i][1] == arrAuxiliarFin[j][1] &&
                        arrJournal[i][2] == arrAuxiliarFin[j][2] &&
                        arrJournal[i][3] == arrAuxiliarFin[j][3] &&
                        arrJournal[i][4] == arrAuxiliarFin[j][4] &&
                        arrJournal[i][5] == arrAuxiliarFin[j][5] &&
                        arrJournal[i][6] == arrAuxiliarFin[j][6] &&
                        arrJournal[i][7] == arrAuxiliarFin[j][7]) {
                        arrAuxiliarFin[j][8] = Number(arrAuxiliarFin[j][8]) + Number(arrJournal[i][8]);
                        banderita = true;
                    }
                }
                if (!banderita) {
                    arrAuxiliarFin.push(arrJournal[i]);
                }
            }
            log.debug('arrAuxiliarFin 1', arrAuxiliarFin);
            for (var i = 0; i < arrInvoice.length; i++) {
                var banderita = false;
                for (var j = 0; j < arrAuxiliarFin.length; j++) {
                    if (arrInvoice[i][0] == arrAuxiliarFin[j][0] &&
                        arrInvoice[i][1] == arrAuxiliarFin[j][1] &&
                        arrInvoice[i][2] == arrAuxiliarFin[j][2] &&
                        arrInvoice[i][3] == arrAuxiliarFin[j][3] &&
                        arrInvoice[i][4] == arrAuxiliarFin[j][4] &&
                        arrInvoice[i][5] == arrAuxiliarFin[j][5] &&
                        arrInvoice[i][6] == arrAuxiliarFin[j][6] &&
                        arrInvoice[i][7] == arrAuxiliarFin[j][7]) {
                        arrAuxiliarFin[j][8] = Number(arrAuxiliarFin[j][8]) + Number(arrInvoice[i][8]);
                        banderita = true;
                    }
                }
                if (!banderita) {
                    arrAuxiliarFin.push(arrInvoice[i]);
                }
            }
            log.debug('arrAuxiliarFin 2', arrAuxiliarFin);
            for (var i = 0; i < arrJourRef.length; i++) {
                var banderita = false;
                for (var j = 0; j < arrAuxiliIva.length; j++) {
                    if (arrJourRef[i][0] == arrAuxiliIva[j][0] &&
                        arrJourRef[i][1] == arrAuxiliIva[j][1] &&
                        arrJourRef[i][2] == arrAuxiliIva[j][2] &&
                        arrJourRef[i][3] == arrAuxiliIva[j][3] &&
                        arrJourRef[i][4] == arrAuxiliIva[j][4] &&
                        arrJourRef[i][5] == arrAuxiliIva[j][5] &&
                        arrJourRef[i][6] == arrAuxiliIva[j][6] &&
                        arrJourRef[i][7] == arrAuxiliIva[j][7]) {
                        arrAuxiliIva[j][9] = Number(arrAuxiliIva[j][9]) + Number(arrJourRef[i][9]);
                        banderita = true;
                    }
                }
                if (!banderita) {
                    //valorTotal = valorTotal + Number(arrJourRef[i][0]);
                    arrAuxiliIva.push(arrJourRef[i]);
                }
            }
            log.debug('arrAuxiliIva 3', arrAuxiliIva);
            log.debug('arrBillCre', arrBillCre);
            for (var i = 0; i < arrBillCre.length; i++) {
                var banderita = false;
                for (var j = 0; j < arrAuxiliIva.length; j++) {
                    if (arrBillCre[i][0] == arrAuxiliIva[j][0] &&
                        arrBillCre[i][1] == arrAuxiliIva[j][1] &&
                        arrBillCre[i][2] == arrAuxiliIva[j][2] &&
                        arrBillCre[i][3] == arrAuxiliIva[j][3] &&
                        arrBillCre[i][4] == arrAuxiliIva[j][4] &&
                        arrBillCre[i][5] == arrAuxiliIva[j][5] &&
                        arrBillCre[i][6] == arrAuxiliIva[j][6] &&
                        arrBillCre[i][7] == arrAuxiliIva[j][7]) {
                        log.debug('ENTROOO', Number(arrBillCre[i][9]));
                        log.debug('ENTROOO2', Number(arrAuxiliIva[j][9]));
                        arrAuxiliIva[j][9] = Number(arrAuxiliIva[j][9]) + Number(arrBillCre[i][9]);
                        log.debug('ENTROOO3', arrAuxiliIva[j][9]);
                        banderita = true;
                    }
                }
                if (!banderita) {
                    //valorTotal = valorTotal + Number(arrBillCre[i][0]);
                    arrAuxiliIva.push(arrBillCre[i]);
                }
            }
            log.debug('arrAuxiliIva 4', arrAuxiliIva);

            for (var i = 0; i < arrAuxiliIva.length; i++) {
                var banderita = false;
                for (var j = 0; j < arrAuxiliarFin.length; j++) {
                    if (arrAuxiliIva[i][0] == arrAuxiliarFin[j][0] &&
                        arrAuxiliIva[i][1] == arrAuxiliarFin[j][1] &&
                        arrAuxiliIva[i][2] == arrAuxiliarFin[j][2] &&
                        arrAuxiliIva[i][3] == arrAuxiliarFin[j][3] &&
                        arrAuxiliIva[i][4] == arrAuxiliarFin[j][4] &&
                        arrAuxiliIva[i][5] == arrAuxiliarFin[j][5] &&
                        arrAuxiliIva[i][6] == arrAuxiliarFin[j][6] &&
                        arrAuxiliIva[i][7] == arrAuxiliarFin[j][7]) {
                        arrAuxiliarFin[j][9] = Number(arrAuxiliarFin[j][9]) + Number(arrAuxiliIva[i][9]);
                        banderita = true;
                    }
                }
                if (!banderita) {
                    valorTotal = valorTotal + Number(arrAuxiliIva[i][0]);
                    ArrLibroMag.push(arrAuxiliIva[i]);
                }
            }
            log.debug('arrAuxiliarFin 5', arrAuxiliarFin);

            for (var i = 0; i < arrAuxiliarFin.length; i++) {
                mayoracuantiasmenores = Math.abs(Number(arrAuxiliarFin[i][8]));
                if (mayoracuantiasmenores > CUANTIA_MINIMA) {
                    valorTotal = valorTotal + Number(arrAuxiliarFin[i][0]);
                    log.debug('ArrLibroMag', ArrLibroMag);
                    ArrLibroMag.push(arrAuxiliarFin[i]);
                } else {
                    existeCuantias = 1;
                    impuesto = impuesto + Number(arrAuxiliarFin[i][8]);
                }
            }

            if (existeCuantias == 1) {
                banderita = true;
                arrAuxiliar = new Array();
                arrAuxiliar[0] = 'Auxiliar';
                arrAuxiliar[1] = banderita;
                arrAuxiliar[2] = valorTotal;
                arrAuxiliar[3] = impuesto;
                ArrLibroMag.push(arrAuxiliar);
            }
        }

        function Agrupado(arr, prop) {
            return arr.reduce(function(groups, item) {
                var val = item[prop];
                groups[val] = groups[val] || { date: item.date, pv: 0, ac: 0, ev: 0 };
                groups[val].pv += item.pv;
                groups[val].ac += item.ac;
                groups[val].ev += item.ev;
                return groups;
            }, {});
        }

        function ObtenerParametrosYFeatures() {

            paramSubsidiaria = objContext.getParameter({
                name: 'custscript_lmry_co_1006_anual_subsi'
            });

            paramMultibook = objContext.getParameter({
                name: 'custscript_lmry_co_1006_anual_multi'
            });

            paramPeriodo = objContext.getParameter({
                name: 'custscript_lmry_co_1006_anual_period'
            });

            paramIdLog = objContext.getParameter({
                name: 'custscript_lmry_co_1006_anual_idlog'
            });

            paramIdReport = objContext.getParameter({
                name: 'custscript_lmry_co_1006_anual_idrpt'
            });

            paramBucle = objContext.getParameter({
                name: 'custscript_lmry_co_1006_anual_bucle'
            });

            paramCont = objContext.getParameter({
                name: 'custscript_lmry_co_1006_anual_conta'
            });

            paramIdFeatureByVersion = objContext.getParameter({
                name: 'custscript_lmry_co_1006_anual_idfbv'
            });

            paramConcepto = objContext.getParameter({
                name: 'custscript_lmry_co_1006_anual_concep'
            });

            IMPUESTOGENERADO = objContext.getParameter({
                name: 'custscript_lmry_co_1006_anual_impgen'
            });

            existeCuantias = objContext.getParameter({
                name: 'custscript_lmry_co_1006_anual_existc'
            });

            if (paramMultibook == null) {
                paramMultibook = '';
            }

            if (paramCont == null) {
                paramCont = 0;
            }

            if (paramBucle == null) {
                paramBucle = 0;
            }

            if (IMPUESTOGENERADO == null) {
                IMPUESTOGENERADO = 0;
            }

            if (existeCuantias == null) {
                existeCuantias = 0;
            }

            isSubsidiariaFeature = runtime.isFeatureInEffect({
                feature: 'SUBSIDIARIES'
            });

            isMultibookFeature = runtime.isFeatureInEffect({
                feature: 'MULTIBOOK'
            });

            isfeatJobs = runtime.isFeatureInEffect({
                feature: "JOBS"
            });

            isAdvanceJobsFeature = runtime.isFeatureInEffect({
                feature: "ADVANCEDJOBS"
            });


            log.error('parametros', paramSubsidiaria + ' - ' + paramMultibook + ' - ' + paramPeriodo + ' - ' + paramIdReport + ' - ' + paramBucle + ' - ' + paramCont + ' - ' + paramIdFeatureByVersion + ' - ' + paramConcepto);

            periodStartDate = new Date(paramPeriodo, 0, 1);
            periodEndDate = new Date(paramPeriodo, 11, 31);

            periodStartDate = format.format({
                value: periodStartDate,
                type: format.Type.DATE
            });

            periodEndDate = format.format({
                value: periodEndDate,
                type: format.Type.DATE
            });

            periodName = paramPeriodo;

            if (isMultibookFeature) {
                var multibook = search.lookupFields({
                    type: search.Type.ACCOUNTING_BOOK,
                    id: paramMultibook,
                    columns: ['name']
                });

                multibookName = multibook.name;
            }

            if (paramIdReport != null) {
                var report = search.lookupFields({
                    type: 'customrecord_lmry_co_features',
                    id: paramIdReport,
                    columns: ['name']
                });
                reportName = report.name;
            }

            if (paramIdFeatureByVersion) {
                var filterByVersion = search.lookupFields({
                    type: 'customrecord_lmry_co_rpt_feature_version',
                    id: paramIdFeatureByVersion,
                    columns: ['custrecord_lmry_co_amount']
                });
                CUANTIA_MINIMA = filterByVersion.custrecord_lmry_co_amount;
            }
        }

        function lengthInUtf8Bytes(str) {
            var m = encodeURIComponent(str).match(/%[89ABab]/g);
            return str.length + (m ? m.length : 0);
        }

        function ObtenerDatosSubsidiaria() {

            if (isSubsidiariaFeature) {
                companyName = ObtainNameSubsidiaria(paramSubsidiaria);
                companyRuc = ObtainFederalIdSubsidiaria(paramSubsidiaria);
            } else {
                var pageConfig = config.load({
                    type: config.Type.COMPANY_INFORMATION
                });

                companyRuc = pageConfig.getFieldValue('employerid');
                companyName = pageConfig.getFieldValue('legalname');
            }
            companyRuc = companyRuc.replace(' ', '');
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
                libreria.sendMail(LMRY_script, ' [ ObtainFederalIdSubsidiaria ] ' + err);
            }

            return '';
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
                libreria.sendMail(LMRY_script, ' [ ObtainNameSubsidiaria ] ' + err);
            }
            return '';
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

        function ObtenerCuantiaMenor() {
            var rate = 1,
                multibookCurrency = '',
                primaryCurrency = '';

            if (paramSubsidiaria && paramMultibook) {
                var savedSearch = search.create({
                    type: 'accountingbook',
                    filters: [
                        ['subsidiary', 'anyof', paramSubsidiaria]
                    ],
                    columns: [
                        search.createColumn({
                            name: 'internalid',
                            sort: search.Sort.ASC
                        }),
                        search.createColumn({
                            name: "currency",
                        }),
                        search.createColumn({
                            name: 'isprimary'
                        })
                    ]
                });

                var searchresult = savedSearch.run();
                var objResult = searchresult.getRange(0, 1000);
                var encontro = false;
                if (objResult != null) {
                    for (var i = 0; i < objResult.length; i++) {
                        var columns = objResult[i].columns;

                        if (!encontro && currenciesJson[objResult[i].getValue(columns[1])][2] == 'COP') {
                            primaryCurrency = objResult[i].getValue(columns[1]);
                            encontro = true;
                        }

                        if (objResult[i].getValue(columns[0]) == paramMultibook) {
                            multibookCurrency = objResult[i].getValue(columns[1]);
                        }
                    }
                }

                log.error('PrimarySymbol', currenciesJson[primaryCurrency]);
                log.error('MultibookSymbol', currenciesJson[multibookCurrency]);

                rate = currency.exchangeRate({
                    source: currenciesJson[primaryCurrency][2],
                    target: currenciesJson[multibookCurrency][2],
                    date: new Date()
                });

                rate = rate || 1;
            }
            log.error('rate', rate);
            CUANTIA_MINIMA = CUANTIA_MINIMA * rate;
        }

        function obtenerNumeroEnvio() {
            var numeroLote = 1;

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

                //            if (loteXRptMgnResult.numeroLote == null) {
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
                });

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

        /* ********************************************************************
         * Generacion de estructura excel y XML
         * ********************************************************************/
        function GenerarExcel(arr) {
            var ArrLibroMag = arr;

            var periodStartDate = "01/01/" + paramPeriodo;

            var periodEndDate = "31/12/" + paramPeriodo;

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
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['titulo'][language] + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row></Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['razonSocial'][language] + ': ' + companyName + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['taxNumber'][language] + ': ' + companyRuc + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['periodo'][language] + ': ' + periodStartDate + ' ' + GLOBAL_LABELS['al'][language] + ' ' + periodEndDate + '</Data></Cell>';
            xlsString += '</Row>';
            if (isMultibookFeature) {
                xlsString += '<Row>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell><Data ss:Type="String">Multibook: ' + multibookName + '</Data></Cell>';
                xlsString += '</Row>';
            }
            // PDF Normalized

            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['origin'][language] + ': Netsuite' + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['date'][language] + ': ' + todays + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['time'][language] + ': ' + currentTime + '</Data></Cell>';
            xlsString += '</Row>';

            // End PDF Normalized

            xlsString += '<Row></Row>';
            xlsString += '<Row></Row>';
            xlsString += '<Row>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> TDOC </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> NID </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> D.V. </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['primerApellido'][language] + '</Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['segApellido'][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['primerNombre'][language] + '</Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['segNombre'][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['razonSocial'][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['ImpGenerado'][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['IVArecuperadoDev'][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['Impuestoalconsumo'][language] + ' </Data></Cell>' +
                '</Row>';

            //creacion de reporte xls
            log.debug('akies', ArrLibroMag);
            for (var i = 0; i < ArrLibroMag.length; i++) {

                var impGen = Math.abs(Number(ArrLibroMag[i][8]).toFixed(0));
                var ivaRecup = Math.abs(Number(ArrLibroMag[i][9]).toFixed(0));
                var impComs = Math.abs(Number(ArrLibroMag[i][10]).toFixed(0));;

                if (impGen > 0 || ivaRecup > 0 || impComs > 0) {

                    xlsString += '<Row>';
                    //0. TDOC
                    if (ArrLibroMag[i][0] != '' && ArrLibroMag[i][0] != null && ArrLibroMag[i][0] != '- None -') {
                        xlsString += '<Cell><Data ss:Type="String">' + ArrLibroMag[i][0] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                    }
                    //1. NID
                    if (ArrLibroMag[i][1] != '' && ArrLibroMag[i][1] != null && ArrLibroMag[i][1] != '- None -') {
                        xlsString += '<Cell><Data ss:Type="String">' + ArrLibroMag[i][1] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                    }
                    //2. D.V.
                    if (ArrLibroMag[i][2] != '' && ArrLibroMag[i][2] != null && ArrLibroMag[i][2] != '- None -') {
                        xlsString += '<Cell><Data ss:Type="String">' + ArrLibroMag[i][2] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                    }
                    //3. 1ER APELL
                    if (ArrLibroMag[i][3] != '' && ArrLibroMag[i][3] != null && ArrLibroMag[i][3].split(' ')[0] != '- None -') {
                        if (ArrLibroMag[i][3].split(' ').length <= 2) {
                            xlsString += '<Cell><Data ss:Type="String">' + ArrLibroMag[i][3].split(' ')[0] + '</Data></Cell>';
                        } else {
                            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        }
                    } else {
                        xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                    }
                    //4. 2DO APELL
                    if (ArrLibroMag[i][4] != '' && ArrLibroMag[i][4] != null && ArrLibroMag[i][4].split(' ')[1] != '- None -') {
                        if (ArrLibroMag[i][4].split(' ').length == 2) {
                            xlsString += '<Cell><Data ss:Type="String">' + ArrLibroMag[i][4].split(' ')[1] + '</Data></Cell>';
                        } else {
                            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        }
                    } else {
                        xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                    }
                    //5. 1ER NOMBRE
                    if (ArrLibroMag[i][5] != '' && ArrLibroMag[i][5] != null && ArrLibroMag[i][5].split(' ')[0] != '- None -') {
                        if (ArrLibroMag[i][5].split(' ').length <= 2) {
                            xlsString += '<Cell><Data ss:Type="String">' + ArrLibroMag[i][5].split(' ')[0] + '</Data></Cell>';
                        } else {
                            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        }
                    } else {
                        xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                    }
                    //6. 2DO NOMBRE
                    if (ArrLibroMag[i][6] != '' && ArrLibroMag[i][6] != null && ArrLibroMag[i][6].split(' ')[0] != '- None -') {
                        if (ArrLibroMag[i][6].split(' ').length == 2) {
                            xlsString += '<Cell><Data ss:Type="String">' + ArrLibroMag[i][6].split(' ')[1] + '</Data></Cell>';
                        } else {
                            xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                        }
                    } else {
                        xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                    }
                    //7. RAZON SOCIAL
                    if (ArrLibroMag[i][7] != '' && ArrLibroMag[i][7] != null && ArrLibroMag[i][7] != '- None -') {
                        xlsString += '<Cell><Data ss:Type="String">' + ArrLibroMag[i][7] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                    }
                    //8. IMPUESTO GENERADO
                    if (ArrLibroMag[i][8] != '' && ArrLibroMag[i][8] != null && ArrLibroMag[i][8] != '- None -') {
                        xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + impGen + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number"></Data></Cell>';
                    }
                    //9. IVA RECUPERADO EN DEVOLUCIONES EN COMPRAS ANULADAS, RESCINDIDAS O RESUELTAS
                    if (ArrLibroMag[i][9] != '' && ArrLibroMag[i][9] != null && ArrLibroMag[i][9] != '- None -') {
                        xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + ivaRecup + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number"></Data></Cell>';
                    }
                    //10. Impuesto al consumo
                    if (ArrLibroMag[i][10] != '' && ArrLibroMag[i][10] != null && ArrLibroMag[i][10] != '- None -') {
                        xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + impComs + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number"></Data></Cell>';
                    }
                    //xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">0</Data></Cell>';
                    xlsString += '</Row>';
                }
            } //fin del quiebre por clase

            if (existeCuantias == 1 && imprimirenExcelXml && Math.abs(Number(IMPUESTOGENERADO).toFixed(0)) > 0) {

                xlsString += '<Row>';
                xlsString += '<Cell><Data ss:Type="String">43</Data></Cell>';
                xlsString += '<Cell><Data ss:Type="String">222222222</Data></Cell>';
                xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                xlsString += '<Cell><Data ss:Type="String">CUANTIAS MENORES</Data></Cell>';
                xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + Math.abs(Number(IMPUESTOGENERADO).toFixed(0)) + '</Data></Cell>';
                xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">0</Data></Cell>';
                xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">0</Data></Cell>';
                xlsString += '</Row>';
            }

            xlsString += '</Table></Worksheet></Workbook>';

            strExcelVentasXPagar = encode.convert({
                string: xlsString,
                inputEncoding: encode.Encoding.UTF_8,
                outputEncoding: encode.Encoding.BASE_64
            });

            SaveFile('.xls');

        }

        function GenerarXml(arr) {
            var ArrLibroMag = arr;
            strXmlVentasXPagar = '';

            var today = new Date();
            var anio = today.getFullYear();
            var mes = completar_cero(2, today.getMonth() + 1);
            var day = completar_cero(2, today.getDay());
            var hour = completar_cero(2, today.getHours());
            var min = completar_cero(2, today.getMinutes());
            var sec = completar_cero(2, today.getSeconds());
            today = anio + '-' + mes + '-' + day + 'T' + hour + ':' + min + ':' + sec;

            if (existeCuantias == 0) {
                cantregistros = ArrLibroMag.length;
            }


            strXmlVentasXPagar += '<?xml version="1.0" encoding="ISO-8859-1"?> \r\n';
            strXmlVentasXPagar += '<mas xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"> \r\n';
            strXmlVentasXPagar += '<Cab> \r\n';
            strXmlVentasXPagar += '<Ano>' + paramPeriodo + '</Ano> \r\n';
            strXmlVentasXPagar += '<CodCpt>' + paramConcepto + '</CodCpt> \r\n';
            strXmlVentasXPagar += '<Formato>1006</Formato> \r\n';
            strXmlVentasXPagar += '<Version>8.1</Version> \r\n';
            strXmlVentasXPagar += '<NumEnvio>' + numeroEnvio + '</NumEnvio> \r\n';
            strXmlVentasXPagar += '<FecEnvio>' + today + '</FecEnvio> \r\n';
            strXmlVentasXPagar += '<FecInicial>' + paramPeriodo + '-01-01</FecInicial> \r\n';
            strXmlVentasXPagar += '<FecFinal>' + paramPeriodo + '-12-31</FecFinal> \r\n';
            strXmlVentasXPagar += '<ValorTotal>' + valorTotal + '</ValorTotal> \r\n';
            strXmlVentasXPagar += '<CantReg>' + cantregistros + '</CantReg> \r\n';
            strXmlVentasXPagar += '</Cab>\r\n';

            for (var i = 0; i < ArrLibroMag.length; i++) {

                var impGen = Math.abs(Number(ArrLibroMag[i][8]).toFixed(0));
                var ivaRecup = Math.abs(Number(ArrLibroMag[i][9]).toFixed(0));
                var impComs = Math.abs(Number(ArrLibroMag[i][10]).toFixed(0));;

                if (impGen > 0 || ivaRecup > 0 || impComs > 0) {

                    strXmlVentasXPagar += '<impoventas icon="' + impComs + '" iva="' + ivaRecup + '" imp="' + impGen;

                    if (ArrLibroMag[i][7] != '' && ArrLibroMag[i][7] != null && ArrLibroMag[i][7] != '- None -') {
                        strXmlVentasXPagar += '" raz="' + ArrLibroMag[i][7] + '" nid="' + ArrLibroMag[i][1] + '" tdoc="' + ArrLibroMag[i][0];

                        if (ArrLibroMag[i][2] != '' && ArrLibroMag[i][2] != null && ArrLibroMag[i][2] != '- None -') {
                            strXmlVentasXPagar += '" dv="' + ArrLibroMag[i][2];
                        }

                    } else {
                        strXmlVentasXPagar += '" nid="' + ArrLibroMag[i][1] + '" tdoc="' + ArrLibroMag[i][0];

                        if (ArrLibroMag[i][5].split(' ')[0] != null && ArrLibroMag[i][5].split(' ')[0] != '') {
                            strXmlVentasXPagar += '" nom1="' + ArrLibroMag[i][5].split(' ')[0];

                            if (ArrLibroMag[i][5].split(' ')[1] != null && ArrLibroMag[i][5].split(' ')[1] != '') {
                                strXmlVentasXPagar += '" nom2="' + ArrLibroMag[i][5].split(' ')[1];
                            }
                        }

                        if (ArrLibroMag[i][3].split(' ')[0] != null && ArrLibroMag[i][3].split(' ')[0] != '') {
                            strXmlVentasXPagar += '" apl1="' + ArrLibroMag[i][3].split(' ')[0];

                            if (ArrLibroMag[i][3].split(' ')[1] != null && ArrLibroMag[i][3].split(' ')[1] != '') {
                                strXmlVentasXPagar += '" apl2="' + ArrLibroMag[i][3].split(' ')[1];
                            }
                        }
                    }

                    strXmlVentasXPagar += '"/> \r\n';

                    //strXmlVentasXPagar += '<impoventas icon="' + Number(ventasXPagarArray[i][10]).toFixed(0) + '" iva="' + Number(ventasXPagarArray[i][9]).toFixed(0) + '" imp="' + Number(ventasXPagarArray[i][8]).toFixed(0) + '" raz="' + ventasXPagarArray[i][7] + '" dv="' + ventasXPagarArray[i][2] + '" nid="' + ventasXPagarArray[i][1] + '" tdoc="' + ventasXPagarArray[i][0] + '"/> \r\n';
                }
            }
            if (existeCuantias == 1 && imprimirenExcelXml && Math.abs(Number(IMPUESTOGENERADO).toFixed(0)) > 0) {
                strXmlVentasXPagar += '<impoventas icon="' + Number(0).toFixed(0) + '" iva="' + Number(0).toFixed(0) + '" imp="' + Math.abs(Number(IMPUESTOGENERADO).toFixed(0));
                strXmlVentasXPagar += '" raz="' + 'CUANTIAS MENORES' + '" nid="' + '222222222' + '" tdoc="' + '43' + '" dv= "';
                strXmlVentasXPagar += '"/> \r\n';
            }
            strXmlVentasXPagar += '</mas> \r\n';

            SaveFile('.xml');
        }

        function ObtenerJournals() {
            var intMinReg = Number(paramBucle) * CANT_REGISTROS;
            var intMaxReg = intMinReg + CANT_REGISTROS;
            var DbolStop = false;
            var arrTransac = [];

            // LatamReady - CO Form 1006 Journals
            var savedSearch = search.load({
                id: 'customsearch_lmry_co_form1006_jrnl'
            });

            if (paramPeriodo) {
                var fechInicioFilter = search.createFilter({
                    name: 'trandate',
                    operator: search.Operator.ONORAFTER,
                    values: [periodStartDate]
                });
                savedSearch.filters.push(fechInicioFilter);

                var fechFinFilter = search.createFilter({
                    name: 'trandate',
                    operator: search.Operator.ONORBEFORE,
                    values: [periodEndDate]
                });
                savedSearch.filters.push(fechFinFilter);
            }

            if (isSubsidiariaFeature) {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [paramSubsidiaria]
                });
                savedSearch.filters.push(subsidiaryFilter);
            }

            if (isMultibookFeature) {
                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [paramMultibook]
                });
                savedSearch.filters.push(multibookFilter);


            }

            var lineUniqueKeyFilter = search.createFilter({
                name: "formulatext",
                formula: "CASE WHEN {lineuniquekey} = {custrecord_lmry_br_transaction.custrecord_lmry_lineuniquekey} THEN 1 ELSE 0 END",
                operator: search.Operator.IS,
                values: '1'
            });
            savedSearch.filters.push(lineUniqueKeyFilter);

            var noReteFilter = search.createFilter({
                name: "formulatext",
                formula: "CASE WHEN NVL({custrecord_lmry_br_transaction.custrecord_lmry_tax_type.id},0) = 1 THEN 0 ELSE 1 END",
                operator: search.Operator.IS,
                values: '1'
            });
            savedSearch.filters.push(noReteFilter);

            var accountFilter = search.createFilter({
                name: "formulatext",
                formula: "{account.custrecord_lmry_co_puc_formatgy}",
                operator: search.Operator.CONTAINS,
                values: 'Impuesto a las ventas por pagar (Generado)'
            });
            savedSearch.filters.push(accountFilter);



            var searchresult = savedSearch.run();

            while (!DbolStop) {

                var objResult = searchresult.getRange(intMinReg, intMaxReg);

                log.error('objResult len ', objResult.length);

                if (objResult != null) {
                    var intLength = objResult.length;

                    if (intLength != CANT_REGISTROS) {
                        DbolStop = true;
                    }

                    for (var i = 0; i < intLength; i++) {

                        var columns = objResult[i].columns;

                        arrAuxiliar = new Array();

                        //TDOC
                        if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -') {
                            arrAuxiliar[0] = objResult[i].getValue(columns[0]);
                        } else {
                            arrAuxiliar[0] = '0';
                        }
                        //1. NID
                        if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -') {
                            arrAuxiliar[1] = RetornaNumero(objResult[i].getValue(columns[1]));
                        } else {
                            arrAuxiliar[1] = '';
                        }
                        //2. D.V.
                        if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -') {
                            arrAuxiliar[2] = RecortarCaracteres(objResult[i].getValue(columns[2]), 1);
                        } else {
                            arrAuxiliar[2] = '0';
                        }
                        //3. 1ER APELLI
                        if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -') {
                            arrAuxiliar[3] = validarAcentos(objResult[i].getValue(columns[3]));
                        } else {
                            arrAuxiliar[3] = '';
                        }
                        //4. 2DO APELLI
                        if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -') {
                            arrAuxiliar[4] = validarAcentos(objResult[i].getValue(columns[4]));
                        } else {
                            arrAuxiliar[4] = '';
                        }
                        //5. 1ER NOMBRE
                        if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -') {
                            arrAuxiliar[5] = validarAcentos(objResult[i].getValue(columns[5]));
                        } else {
                            arrAuxiliar[5] = '';
                        }
                        //6. 2DO NOMBRE
                        if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -') {
                            arrAuxiliar[6] = validarAcentos(objResult[i].getValue(columns[6]));
                        } else {
                            arrAuxiliar[6] = '';
                        }
                        //7. RAZON SOCIAL
                        if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -') {
                            arrAuxiliar[7] = validarAcentos(objResult[i].getValue(columns[7]));
                        } else {
                            arrAuxiliar[7] = '';
                        }
                        //8. IMPUESTO GENERADO
                        if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -') {
                            arrAuxiliar[8] = Math.abs(Number(objResult[i].getValue(columns[8]))).toFixed(0);

                        } else {
                            arrAuxiliar[8] = 0;
                        }
                        //9. IVA RECUPERADO EN DEVOLUCIONES EN COMPRAS ANULADAS, RESCINDIDAS O RESUELTAS
                        if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -') {
                            arrAuxiliar[9] = Math.abs(Number(objResult[i].getValue(columns[9]))).toFixed(0);
                        } else {
                            arrAuxiliar[9] = 0;
                        }
                        //10. IMPUESTO AL CONSUMO
                        if (objResult[i].getValue(columns[10]) != null && objResult[i].getValue(columns[10]) != '- None -') {
                            arrAuxiliar[10] = Math.abs(Number(objResult[i].getValue(columns[10]))).toFixed(0);
                        } else {
                            arrAuxiliar[10] = 0;
                        }

                        arrTransac.push(arrAuxiliar);
                    }
                    if (!DbolStop) {
                        intMinReg = intMaxReg;
                        intMaxReg += CANT_REGISTROS;
                        paramBucle = Number(paramBucle) + 1;
                    }
                } else {
                    DbolStop = true;
                }
            }
            log.debug('journals aceptados XDD', arrTransac);
            return arrTransac;
        }

        function ObtenerTransaction(tipo) {
            var arrTransac = new Array();
            // LatamReady - CO Form 1006 Transaction
            var savedSearch = search.load({
                id: 'customsearch_lmry_co_form1006_tran'
            });

            if (tipo == 'Inv') {
                var typeTransa = search.createFilter({
                    name: "type",
                    operator: search.Operator.ANYOF,
                    values: ["CustInvc"]
                });
                savedSearch.filters.push(typeTransa);
            } else {
                var typeTransa = search.createFilter({
                    name: "type",
                    operator: search.Operator.ANYOF,
                    values: ["VendCred"]
                });
                savedSearch.filters.push(typeTransa);


                var modificationCode = search.createFilter({
                    name: "formulatext",
                    formula: "{custbody_lmry_modification_reason.custrecord_lmry_cod_modification_reason}",
                    operator: search.Operator.ISNOT,
                    values: ["6"]
                })
                savedSearch.filters.push(modificationCode);
            }

            if (paramPeriodo) {
                var fechInicioFilter = search.createFilter({
                    name: 'trandate',
                    operator: search.Operator.ONORAFTER,
                    values: [periodStartDate]
                });
                savedSearch.filters.push(fechInicioFilter);

                var fechFinFilter = search.createFilter({
                    name: 'trandate',
                    operator: search.Operator.ONORBEFORE,
                    values: [periodEndDate]
                });
                savedSearch.filters.push(fechFinFilter);
            }

            if (isSubsidiariaFeature) {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [paramSubsidiaria]
                });
                savedSearch.filters.push(subsidiaryFilter);
            }

            if (isMultibookFeature) {
                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [paramMultibook]
                });
                savedSearch.filters.push(multibookFilter);

                var saldoColumn = search.createColumn({
                    name: 'formulanumeric',
                    summary: "SUM",
                    formula: 'NVL({accountingtransaction.debitamount},0)-NVL({accountingtransaction.creditamount},0)'
                });
                savedSearch.columns.push(saldoColumn);
            }

            var montoFilter = search.createFilter({
                name: 'formulatext',
                formula: "NVL({debitamount},0) - NVL({creditamount},0)",
                operator: search.Operator.ISNOT,
                values: '0'
            });
            savedSearch.filters.push(montoFilter);

            var pagedData = savedSearch.runPaged({
                pageSize: 1000
            });

            var page, columns;

            pagedData.pageRanges.forEach(function(pageRange) {
                page = pagedData.fetch({
                    index: pageRange.index
                });

                page.data.forEach(function(result) {
                    columns = result.columns;
                    arrAuxiliar = new Array();

                    // 0. ID CUSTOMER
                    var column1 = result.getValue(columns[0]);

                    if (tipo == 'Inv') {
                        var aux = search.Type.CUSTOMER;
                    } else {
                        var aux = search.Type.VENDOR;
                    }

                    var customerFields = search.lookupFields({
                        type: aux,
                        id: column1,
                        columns: ['custentity_lmry_sunat_tipo_doc_cod', 'vatregnumber', 'custentity_lmry_sunat_tipo_doc_id', 'custentity_lmry_digito_verificator', 'isperson']
                    });

                    arrAuxiliar[0] = customerFields.custentity_lmry_sunat_tipo_doc_cod;
                    arrAuxiliar[1] = RetornaNumero(customerFields.vatregnumber);

                    if (customerFields.custentity_lmry_sunat_tipo_doc_id && customerFields.custentity_lmry_sunat_tipo_doc_id != '' && (customerFields.custentity_lmry_sunat_tipo_doc_id[0]).value == '12') {
                        arrAuxiliar[2] = RecortarCaracteres(customerFields.custentity_lmry_digito_verificator, 1);
                    } else {
                        arrAuxiliar[2] = '0';
                    }
                    if (customerFields.isperson == true) {
                        var customerPerson = search.lookupFields({
                            type: aux,
                            id: column1,
                            columns: ['lastname', 'firstname']
                        });

                        arrAuxiliar[3] = validarAcentos(customerPerson.lastname);
                        arrAuxiliar[4] = validarAcentos(customerPerson.lastname);
                        arrAuxiliar[5] = validarAcentos(customerPerson.firstname);
                        arrAuxiliar[6] = validarAcentos(customerPerson.firstname);
                        arrAuxiliar[7] = '';
                    } else {
                        var customerPerson = search.lookupFields({
                            type: aux,
                            id: column1,
                            columns: ['companyname']
                        });
                        arrAuxiliar[3] = '';
                        arrAuxiliar[4] = '';
                        arrAuxiliar[5] = '';
                        arrAuxiliar[6] = '';
                        arrAuxiliar[7] = validarAcentos(customerPerson.companyname);
                    }

                    if (tipo == 'Inv') {
                        // 1. IMPUESTO GENERADO
                        if (isMultibookFeature) {
                            arrAuxiliar[8] = Math.abs(Number(result.getValue(columns[2]))).toFixed(0);
                        } else {
                            arrAuxiliar[8] = Math.abs(Number(result.getValue(columns[1]))).toFixed(0);
                        }

                        // 2. IVA
                        arrAuxiliar[9] = 0;

                        // 3. IMPUESTO AL CONSUMO
                        arrAuxiliar[10] = 0;
                    } else {
                        // 1. IMPUESTO GENERADO
                        arrAuxiliar[8] = 0;

                        // 2. IVA
                        if (isMultibookFeature) {
                            arrAuxiliar[9] = Math.abs(Number(result.getValue(columns[2]))).toFixed(0);
                        } else {
                            arrAuxiliar[9] = Math.abs(Number(result.getValue(columns[1]))).toFixed(0);
                        }

                        // 3. IMPUESTO AL CONSUMO
                        arrAuxiliar[10] = 0;
                    }

                    arrTransac.push(arrAuxiliar);

                });
            });

            var aux = arrTransac.length;
            log.debug('Arr Transaction', arrTransac);

            return arrTransac;
        }

        function ObtenerJournalsReferences() {
            var arrTransac = new Array();
            // LatamReady - CO Form 1006 Journals References
            var savedSearch = search.load({
                id: 'customsearch_lmry_co_form1006_jrnl_refe'
            });

            if (paramPeriodo) {
                var fechInicioFilter = search.createFilter({
                    name: 'trandate',
                    operator: search.Operator.ONORAFTER,
                    values: [periodStartDate]
                });
                savedSearch.filters.push(fechInicioFilter);

                var fechFinFilter = search.createFilter({
                    name: 'trandate',
                    operator: search.Operator.ONORBEFORE,
                    values: [periodEndDate]
                });
                savedSearch.filters.push(fechFinFilter);
            }

            if (isSubsidiariaFeature) {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [paramSubsidiaria]
                });
                savedSearch.filters.push(subsidiaryFilter);
            }

            if (isMultibookFeature) {
                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [paramMultibook]
                });
                savedSearch.filters.push(multibookFilter);

                var saldoColumn = search.createColumn({
                    name: 'formulanumeric',
                    summary: "SUM",
                    formula: 'NVL({accountingtransaction.creditamount},0)'
                });
                savedSearch.columns.push(saldoColumn);
            }

            var montoFilter = search.createFilter({
                name: 'formulatext',
                formula: "NVL({creditamount},0)",
                operator: search.Operator.ISNOT,
                values: '0'
            });
            savedSearch.filters.push(montoFilter);

            var referenceTrue = search.createFilter({
                name: 'mainline',
                join: 'custbody_lmry_reference_transaction',
                operator: search.Operator.IS,
                values: 'T'
            });
            savedSearch.filters.push(referenceTrue);


            var referenceTransa = search.createFilter({
                name: 'type',
                join: 'custbody_lmry_reference_transaction',
                operator: search.Operator.ANYOF,
                values: "VendCred"
            });
            savedSearch.filters.push(referenceTransa);


            var pagedData = savedSearch.runPaged({
                pageSize: 1000
            });

            var page, columns;

            pagedData.pageRanges.forEach(function(pageRange) {
                page = pagedData.fetch({
                    index: pageRange.index
                });

                page.data.forEach(function(result) {
                    columns = result.columns;
                    arrAuxiliar = new Array();
                    //0. TDOC
                    if (result.getValue(columns[0]) && result.getValue(columns[0]) != '- None -') {
                        arrAuxiliar[0] = result.getValue(columns[0]);
                    } else {
                        arrAuxiliar[0] = '';
                    }

                    //1. NID
                    if (result.getValue(columns[1]) && result.getValue(columns[1]) != '- None -') {
                        arrAuxiliar[1] = RetornaNumero(result.getValue(columns[1]));
                    } else {
                        arrAuxiliar[1] = '';
                    }

                    //2. D.V.
                    if (result.getValue(columns[2]) && result.getValue(columns[2]) != '- None -') {
                        arrAuxiliar[2] = RecortarCaracteres(result.getValue(columns[2]), 1);
                    } else {
                        arrAuxiliar[2] = '0';
                    }

                    //3. 1ER APELLI
                    if (result.getValue(columns[3]) && result.getValue(columns[3]) != '- None -') {
                        arrAuxiliar[3] = validarAcentos(result.getValue(columns[3]));
                    } else {
                        arrAuxiliar[3] = '';
                    }

                    //4. 2DO APELLI
                    if (result.getValue(columns[4]) && result.getValue(columns[4]) != '- None -') {
                        arrAuxiliar[4] = validarAcentos(result.getValue(columns[4]));
                    } else {
                        arrAuxiliar[4] = '';
                    }

                    //5. 1ER NOMBRE
                    if (result.getValue(columns[5]) && result.getValue(columns[5]) != '- None -') {
                        arrAuxiliar[5] = validarAcentos(result.getValue(columns[5]));
                    } else {
                        arrAuxiliar[5] = '';
                    }

                    //6. 2DO NOMBRE
                    if (result.getValue(columns[6]) && result.getValue(columns[6]) != '- None -') {
                        arrAuxiliar[6] = validarAcentos(result.getValue(columns[6]));
                    } else {
                        arrAuxiliar[6] = '';
                    }

                    //7. RAZON SOCIAL
                    if (result.getValue(columns[7]) && result.getValue(columns[7]) != '- None -') {
                        arrAuxiliar[7] = validarAcentos(result.getValue(columns[7]));
                    } else {
                        arrAuxiliar[7] = '';
                    }

                    //8. IMPUESTO GENERADO
                    arrAuxiliar[8] = 0;

                    //9. IVA RECUPERADO
                    if (isMultibookFeature) {
                        if (result.getValue(columns[9]) && result.getValue(columns[9]) != '- None -') {
                            arrAuxiliar[9] = Math.abs(Number(result.getValue(columns[9]))).toFixed(0);
                        } else {
                            arrAuxiliar[9] = 0;
                        }
                    } else {
                        if (result.getValue(columns[8]) && result.getValue(columns[8]) != '- None -') {
                            arrAuxiliar[9] = Math.abs(Number(result.getValue(columns[8]))).toFixed(0);
                        } else {
                            arrAuxiliar[9] = 0;
                        }
                    }

                    //10. IMPUESTO AL CONSUMO
                    arrAuxiliar[10] = 0;

                    arrTransac.push(arrAuxiliar);
                });
            });

            var aux = arrTransac.length;
            log.debug('References', arrTransac);

            return arrTransac;
        }

        function validarAcentos(s) {
            var AccChars = "ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðñòóôõöùúûüýÿ&°–—ªº·";
            var RegChars = "SZszYAAAAAACEEEEIIIIDNOOOOOUUUUYaaaaaaceeeeiiiidnooooouuuuyyyo--ao.";

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

        function RetornaNumero(nid) {
            if (nid != null && nid != '') {
                return nid.replace(/(\.|-|\/)/g, '');
            }
            return '';
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

        function RecortarCaracteres(valor, numero) {
            if (valor != null && valor.length > numero) {
                return valor.substring(0, numero);
            }
            return valor;
        }

        function Name_File() {
            var name = '';

            name = 'Dmuisca_' + completar_cero(2, paramConcepto) + '01006' + '81' + paramPeriodo + completar_cero(8, numeroEnvio);

            return name;
        }

        function SaveFile(extension) {
            var folderId = objContext.getParameter({
                name: 'custscript_lmry_file_cabinet_rg_co'
            });

            // Almacena en la carpeta de Archivos Generados
            if (folderId != '' && folderId != null) {
                // Extension del archivo
                var fileName = Name_File() + extension;

                // Crea el archivo
                var ventasXPagarFile;

                if (extension == '.xls') {
                    ventasXPagarFile = file.create({
                        name: fileName,
                        fileType: file.Type.EXCEL,
                        contents: strExcelVentasXPagar,
                        folder: folderId
                    });

                } else {
                    ventasXPagarFile = file.create({
                        name: fileName,
                        fileType: file.Type.PLAINTEXT,
                        contents: strXmlVentasXPagar,
                        encoding: file.Encoding.UTF8,
                        folder: folderId
                    });
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

                log.debug({
                    title: 'URL ARCHIVO TEMP',
                    details: fileUrl
                });

                if (fileId) {
                    var usuario = runtime.getCurrentUser();
                    var employee = search.lookupFields({
                        type: search.Type.EMPLOYEE,
                        id: usuario.id,
                        columns: ['firstname', 'lastname']
                    });
                    var usuarioName = employee.firstname + ' ' + employee.lastname;

                    if (Number(paramCont) > 1 || generarXml) {
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
                        value: periodName
                    });

                    if (isMultibookFeature) {
                        //Multibook
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
        }


        function NoData() {
            var usuario = runtime.getCurrentUser();

            var employee = search.lookupFields({
                type: search.Type.EMPLOYEE,
                id: usuario.id,
                columns: ['firstname', 'lastname']
            });
            var usuarioName = employee.firstname + ' ' + employee.lastname;

            if (Number(paramCont) > 1) {
                var generatorLog = record.create({
                    type: 'customrecord_lmry_co_rpt_generator_log'
                });
            } else {
                var generatorLog = record.load({
                    type: 'customrecord_lmry_co_rpt_generator_log',
                    id: paramIdLog
                });
            }

            //Nombre de Archivo
            generatorLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_name',
                value: 'No existe informacion para los criterios seleccionados.'
            });

            //Nombre de Reporte
            generatorLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_transaction',
                value: reportName
            });

            //Nombre de Subsidiaria
            generatorLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_subsidiary',
                value: companyName
            });

            //Periodo
            generatorLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_postingperiod',
                value: periodName
            });

            if (isMultibookFeature) {
                //Multibook
                generatorLog.setValue({
                    fieldId: 'custrecord_lmry_co_rg_multibook',
                    value: multibookName
                });
            }

            //Creado Por
            generatorLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_employee',
                value: usuarioName
            });

            var recordId = generatorLog.save();
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
            //reduce: reduce,
            summarize: summarize
        };

    });