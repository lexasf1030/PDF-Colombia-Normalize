/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_InvBalance_v2_SCHDL_v2.0.js              ||
||                                                              ||
||  Version Date         Author        Remarks                  ||
||  2.0    Jun 09 2021  LatamReady    Use Script 2.0            ||
 \= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.0
 * @NScriptType ScheduledScript
 * @NModuleScope Public
 */
define(["N/record", "N/runtime", "N/file", "N/encode", "N/search",
    "N/format", "N/log", "N/config", "N/task", 'N/render', "N/query", "N/xml",
    "/SuiteBundles/Bundle 37714/Latam_Library/LMRY_LibraryReport_LBRY_V2.js"
],

    function (recordModulo, runtime, fileModulo, encode, search, format, log,
        config, task, render, query, xml, libFeature) {

        var objContext = runtime.getCurrentScript();
        //var namereport = "Reportes Libro de Inventario y Balance";
        var LMRY_script = 'LMRY CO Reportes Libro de Inventario y Balance SCHDL 2.0';
        //Parametros
        var paramSubsidi = '';
        var paramPeriodo = '';
        var paramPeriodsRestantes = '';
        var paramLogId = '';
        var paramMulti = '';
        var paramPUC = '';
        var paramFile = '';
        var paramAdjustment = '';
        var paramDigits = '';

        var error8digitos = '';

        //Control de Reporte
        var periodstartdate = '';
        var periodenddate = '';
        var companyruc = '';
        var companyname = '';

        var xlsString = '';
        var jsonDescriptionPuc8 = {};
        var jsonParametros = {};


        var ArrMovimientos = new Array();
        var SaldoAnteriorPUC = new Array();
        var arrAccountingContext = new Array();
        var jsonAccounts = {};
        var SaldoAnterior = new Array();

        var periodname = '';
        var Final_string;
        var multibookName = '';
        var language;
        var Fecha_Corte_al;
        var PeriodosRestantes = new Array();
        var Pucs = new Array();
        var calendarSubsi;
        //Features
        var featSubsi = null;
        var featMulti = null;
        var featurePeriodEnd = null;
        var featureCalendars = null;
        var featAccountingSpecial = null;

        var GLOBAL_LABELS = {};

        //PDF Normalization
        var todays = "";
        var currentTime = "";

        function execute(context) {
            try {

                GLOBAL_LABELS = getGlobalLabels();
                var language = runtime.getCurrentScript().getParameter({
                    name: 'LANGUAGE'
                }).substring(0, 2);
                ObtenerParametrosYFeatures();
                if (error8digitos) {
                    return false;
                }
                PeriodosRestantes = paramPeriodsRestantes.split(',');
                PeriodosRestantes = PeriodosRestantes.map(function e(p) {
                    return Number(p)
                });
                //obtener saldo anterior
                obtenerDataAnterior(); //obtiene data de archivo temporal, seteando en 2 variables SaldoAnteriorPUC, SaldoAnterior
                //obtener Movimientos
                var ArrMovimientos = [];

                if (PeriodosRestantes.length != 0) {

                    log.debug('Scripts params:', jsonParametros);

                    jsonAccounts = ObtenerCuentas();

                    log.debug('jsonAccounts:', jsonAccounts);

                    // var transactionFile = file.create({
                    //     name: 'jsonAccounts.json',
                    //     fileType: file.Type.JSON,
                    //     contents: JSON.stringify(jsonAccounts),
                    //     folder: -15
                    // }).save();

                    PeriodosRestantes = dividirArray(PeriodosRestantes, 3);

                    for (var x = 0; x < PeriodosRestantes.length; x++) {
                        var arrMovimientosAux = [];
                        arrMovimientosAux = ObtieneTransacciones(PeriodosRestantes[x]);
                        ArrMovimientos = ArrMovimientos.concat(arrMovimientosAux);
                        log.debug('Periodos aux ', PeriodosRestantes[x]);
                        log.debug('ArrMovimientosAux tam', arrMovimientosAux.length);
                    }

                    //arrMovimientosAux = ObtieneDataSet(PeriodosRestantes[x]);
                    //  ArrMovimientos = ObtieneTransacciones();
                    log.debug('ArrMovimientos', ArrMovimientos.length);

                    if ((paramAdjustment == true || paramAdjustment == 'T') && (featurePeriodEnd || featurePeriodEnd == 'T')) {
                        var dataEndJournal = ObtienePeriodEndJournal();
                        log.debug('dataEndJournal', dataEndJournal.length);

                        ArrMovimientos = ArrMovimientos.concat(dataEndJournal);
                    }
                    // log.debug('ArrMovimientos tot', ArrMovimientos);

                    if (featMulti) {
                        ArrMovimientos = SetPUCMultibook(ArrMovimientos); //usa jsonAccounts
                    }

                    if (featMulti) {
                        var ArrMovimientosSpecific = ObtieneSpecificTransaction();
                        log.debug('ArrMovimientosSpecific', ArrMovimientosSpecific.length);

                        ArrMovimientos = ArrMovimientos.concat(ArrMovimientosSpecific);


                        // if (!ValidatePrimaryBook() || ValidatePrimaryBook() != 'T') {
                        //     ObtieneAccountingContext();
                        //     CambioDeCuentas(ArrMovimientos); //usa arrAccountingContext
                        // }
                    }

                    log.debug('ArrMovimientos antes', ArrMovimientos);
                    if (ArrMovimientos.length > 1) {
                        ArrMovimientos = OrdenarCuentas(ArrMovimientos);
                        ArrMovimientos = AgruparCuentas(ArrMovimientos);
                        // Los movimiento agrupoados por ID de 4 digitos y sumando los montos
                    }
                    log.debug('ArrMovimientos despues', ArrMovimientos);
                }

                //juntar arreglos de movimiento y saldo anterior
                // Obtiene COD 4D Y DESCRIPCIONES (4D, 2D Y 1D)
                Pucs = obtenerDescripPUC();
                log.debug('obtenerDescripPUC', Pucs);
                var arrTotal = juntarSaldoYMovimiento(SaldoAnteriorPUC, ArrMovimientos);
                log.debug('saldoo anterior y movimientos', arrTotal.length);

                // log.debug('arrTotal', arrTotal);
                // SaldoAnterior todos los Pucs recorridos hasta ese momento, y arrTotal el Puc que esta recorriendo
                // log.debug('saldo anterior',SaldoAnterior);
                var dataTotal = SaldoAnterior.concat(arrTotal);
                // log.debug('dataTotal', dataTotal);

                log.debug('dataTotal', dataTotal);

                if (paramPUC == '9') {
                    ObtenerDatosSubsidiaria();
                    dataTotal = dataTotal.filter(function (v) {
                        return v[3] != 0;
                    });
                    if (paramDigits == 2) {
                        var arr6digits = [];
                        var arr4digits = agruparNivel4(dataTotal);
                        var arr2digits = agruparNivel2(arr4digits);
                        var arr1digits = agruparNivel1(arr2digits);
                    } else if (paramDigits == 3) {
                        var arr6digits = agruparNivel6(dataTotal);
                        var arr4digits = agruparNivel4(arr6digits);
                        var arr2digits = agruparNivel2(arr4digits);
                        var arr1digits = agruparNivel1(arr2digits);
                    } else {
                        var arr6digits = [];
                        var arr4digits = [];
                        var arr2digits = agruparNivel2(dataTotal);
                        var arr1digits = agruparNivel1(arr2digits);
                    }

                    log.debug('arr6digits', arr6digits);
                    log.debug('arr4digits', arr4digits);
                    log.debug('arr2digits', arr2digits);
                    log.debug('arr1digits', arr1digits);

                    //Generar excel final del reporte
                    if (arr1digits.length != 0) {

                        todays = parseDateTo(new Date(), "DATE");
                        currentTime = getTimeHardcoded(parseDateTo(new Date(), "DATETIME"));

                        if (paramDigits == 2) {
                            GenerarExcel6Digitos(dataTotal, arr4digits, arr2digits, arr1digits);
                        } else if (paramDigits == 3) {
                            GenerarExcel8Digitos(dataTotal, arr6digits, arr4digits, arr2digits, arr1digits);
                        } else {
                            GenerarExcel(dataTotal, arr2digits, arr1digits);
                        }
                        generarPDF(arr1digits, arr2digits, arr4digits, arr6digits, dataTotal);
                    } else {
                        updateReportLog(GLOBAL_LABELS['noData'][language]);
                    }
                } else {
                    //actualizar archivo temporal
                    if (dataTotal.length != 0) {
                        var nameFile = 'INVENTARIO_BALANCE_TEMPORAL';
                        saveFile(formatear(dataTotal), nameFile, 'txt');
                    } else {
                        // log.debug('No hay data ni de saldos ni de movimientos.', 'No se actualiza archivo.')
                    }
                    //llamar de nuevo a map reduce con el siguiente numero PUC
                    paramPUC++;
                    llamarMapReduce();
                }

            } catch (err) {
                log.error('error execute', err);
                //libreria.sendMail(LMRY_script, ' [ execute ] ' + err);
                updateReportLog(GLOBAL_LABELS['errorMsg'][language]);
            }

        }

        function getGlobalLabels() {
            var labels = {
                "titulo": {
                    "es": 'LIBRO DE INVENTARIO Y BALANCE',
                    "pt": 'LIVRO DE INVENTÁRIO E BALANÇO',
                    "en": 'INVENTORY AND BALANCE BOOK'
                },
                "razonSocial": {
                    "es": 'Razon Social',
                    "pt": 'Razão social',
                    "en": 'Business name'
                },
                "corteAl": {
                    "es": 'Corte al',
                    "pt": 'Corta para',
                    "en": 'Cut to'
                },
                "cuenta": {
                    "es": 'Cuenta',
                    "pt": 'Conta',
                    "en": 'Account'
                },
                "denominacion": {
                    "es": 'Denominación',
                    "pt": 'Denominação',
                    "en": 'Denomination'
                },
                "importe": {
                    "es": 'Importe',
                    "pt": 'Quantia',
                    "en": 'Amount'
                },
                'noData': {
                    "es": 'No existe informacion para los criterios seleccionados',
                    "pt": 'Não há informações para os critérios selecionados',
                    "en": 'There is no information for the selected criteria'
                },
                'errorMsg': {
                    "es": 'Ocurrió un error inesperado en el reporte.',
                    "pt": 'Ocorreu um erro inesperado no relatório.',
                    "en": 'An unexpected error occurred in the report.'
                },
                'diferencia': {
                    "es": 'DIFERENCIA',
                    "pt": 'DIFERENÇA',
                    "en": 'DIFFERENCE'
                },
                "origin": {
                  "es": "Origen: ",
                  "en": "Origin: ",
                  "pt": "Origem: "
                },
                "date": {
                  "es": "Fecha: ",
                  "en": "Date: ",
                  "pt": "Data: "
                },
                "time": {
                  "es": "Hora: ",
                  "en": "Time: ",
                  "pt": "Hora: "
                },
                "page": {
                  "es": "Página",
                  "en": "Page",
                  "pt": "Página"
                },
                "of": {
                  "es": "de",
                  "en": "of",
                  "pt": "de"
                }
            }

            return labels;
        }

        function ObtienePeriodEndJournal() {
            // Control de Memoria
            var intDMaxReg = 1000;
            var intDMinReg = 0;
            var arrQuiebre = new Array();
            // Exedio las unidades
            var DbolStop = false;
            var arrCuatroDigitos = new Array();
            var arrSeisDigitos = new Array();
            var arrOchoDigitos = new Array();
            var _contg = 0;


            var savedsearch = search.load({
                /*LatamReady - CO Inventory Book and Balance Period End Journal*/
                id: 'customsearch_lmry_co_invent_balanc_pej'
            });

            var confiPeriodEnd = search.createSetting({
                name: 'includeperiodendtransactions',
                value: 'TRUE'
            })
            savedsearch.settings.push(confiPeriodEnd);

            var periodosSTR = PeriodosRestantes.toString();
            var periodFilterFROM = search.createFilter({
                name: 'formulanumeric',
                formula: 'CASE WHEN {postingperiod.id} IN (' + periodosSTR + ') THEN 1 ELSE 0 END',
                operator: search.Operator.EQUALTO,
                values: [1]
            });
            savedsearch.filters.push(periodFilterFROM);

            if (featSubsi) {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [paramSubsidi]
                });
                savedsearch.filters.push(subsidiaryFilter);
            }

            if (featMulti) {
                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [paramMulti]
                });
                savedsearch.filters.push(multibookFilter);
                //11.
                var balanceColumn = search.createColumn({
                    name: 'formulacurrency',
                    summary: "SUM",
                    formula: "NVL({accountingtransaction.debitamount},0) - NVL({accountingtransaction.creditamount},0)"
                });
                savedsearch.columns.push(balanceColumn);
                //12
                var exchangerateColum = search.createColumn({
                    name: 'formulacurrency',
                    summary: "GROUP",
                    formula: "{accountingtransaction.exchangerate}"
                });
                savedsearch.columns.push(exchangerateColum);
                //13
                if (paramDigits == 2) {
                    var multiAccountColumn = search.createColumn({
                        name: 'account',
                        join: 'accountingtransaction',
                        summary: "GROUP"
                    });
                    savedsearch.columns.push(multiAccountColumn);
                }
                else {
                    var multiAccountColumn = search.createColumn({
                        name: 'formulanumeric',
                        formula: '{accountingtransaction.account.id}',
                        summary: "GROUP"
                    });
                    savedsearch.columns.push(multiAccountColumn);
                }

                if (paramDigits == 2 || paramDigits == 3) {
                    // 14.
                    seisDigitID = search.createColumn({
                        name: "formulatext",
                        summary: "GROUP",
                        formula: "{account.custrecord_lmry_co_puc_d6_id}",
                        sort: search.Sort.ASC,
                    });
                    savedsearch.columns.push(seisDigitID);

                    //15.
                    seisDescripcionID = search.createColumn({
                        name: "formulatext",
                        summary: "GROUP",
                        formula: "{account.custrecord_lmry_co_puc_d6_description}",
                        sort: search.Sort.ASC,
                    });
                    savedsearch.columns.push(seisDescripcionID);
                }

                if (paramDigits == 3) {
                    // 16.
                    ochoDigitID = search.createColumn({
                        name: "formulatext",
                        summary: "GROUP",
                        formula: "{account.custrecord_lmry_co_puc_id}",
                        sort: search.Sort.ASC,
                    });
                    savedsearch.columns.push(ochoDigitID);
                }

            } else {

                var pucFilter = search.createFilter({
                    name: 'formulatext',
                    formula: '{account.custrecord_lmry_co_puc_d4_id}',
                    operator: search.Operator.STARTSWITH,
                    values: [paramPUC]
                });
                savedsearch.filters.push(pucFilter);

            }

            var searchresult = savedsearch.run();

            while (!DbolStop) {
                var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

                if (objResult != null) {
                    var intLength = objResult.length;

                    if (intLength != 1000) {
                        DbolStop = true;
                    }

                    for (var i = 0; i < intLength; i++) {
                        var columns = objResult[i].columns;
                        arrQuiebre = new Array();

                        //PARA 6 DIGITOS
                        if (paramDigits == 2) {
                            for (var col = 0; col < columns.length - 2; col++) {
                                arrQuiebre[0] = objResult[i].getValue(columns[14]);
                                arrQuiebre[1] = objResult[i].getValue(columns[15]);
                                if (col == 7) {
                                    if (featMulti) {
                                        arrQuiebre[9] = redondear(objResult[i].getValue(columns[11]));
                                    } else {
                                        arrQuiebre[9] = redondear(objResult[i].getValue(columns[7]));
                                    }
                                } else {
                                    arrQuiebre[col + 2] = objResult[i].getValue(columns[col]);
                                }
                            }
                            if (arrQuiebre[9] != 0) {

                                if (featMulti) {
                                    var idAccount = objResult[i].getValue(columns[13]);

                                    if (jsonAccounts[idAccount] != null) {

                                        if (jsonAccounts[idAccount].columna7.substring(0, 1) == paramPUC) {
                                            arrSeisDigitos[_contg] = arrQuiebre;
                                            _contg++;
                                        }

                                    }

                                } else {
                                    arrSeisDigitos[_contg] = arrQuiebre;
                                    _contg++;
                                }
                            }
                        } else if (paramDigits == 3) {
                            //  PARA 8 DIGITOS
                            arrQuiebre[0] = jsonAccounts[objResult[i].getValue(columns[13])]["columna9"];
                            arrQuiebre[1] = jsonAccounts[objResult[i].getValue(columns[13])]["columna10"]; //descripcion8Digit;
                            arrQuiebre[2] = jsonAccounts[objResult[i].getValue(columns[13])]["columna7"];
                            arrQuiebre[3] = jsonAccounts[objResult[i].getValue(columns[13])]["columna8"];
                            for (var col = 0; col < columns.length - 3; col++) {
                                if (col == 7) {
                                    if (featMulti) {
                                        arrQuiebre[11] = redondear(objResult[i].getValue(columns[11]));
                                    } else {
                                        arrQuiebre[11] = redondear(objResult[i].getValue(columns[7]));
                                    }
                                } 
                                else if (col == 13) {
                                    arrQuiebre[17] = objResult[i].getValue(columns[13]);
                                }
                                else {
                                    arrQuiebre[col + 4] = objResult[i].getValue(columns[col]);
                                }
                            }
                            if (arrQuiebre[11] != 0) {

                                if (featMulti) {
                                    var idAccount = objResult[i].getValue(columns[13]);

                                    if (jsonAccounts[idAccount] != null) {

                                        if (jsonAccounts[idAccount].columna9.substring(0, 1) == paramPUC) {
                                            arrOchoDigitos[_contg] = arrQuiebre;
                                            _contg++;
                                        }

                                    }

                                } else {
                                    arrOchoDigitos[_contg] = arrQuiebre;
                                    _contg++;
                                }
                            }
                        } else {
                            //PARA 4 DIGITOS
                            for (var col = 0; col < columns.length; col++) {
                                if (col == 7) {
                                    if (featMulti) {
                                        arrQuiebre[col] = redondear(objResult[i].getValue(columns[11]));
                                    } else {
                                        arrQuiebre[col] = redondear(objResult[i].getValue(columns[7]));
                                    }
                                } else {
                                    arrQuiebre[col] = objResult[i].getValue(columns[col]);
                                }
                            }

                            if (featMulti) {
                                var idAccount = objResult[i].getValue(columns[13]);

                                if (jsonAccounts[idAccount] != null) {
                                    if (jsonAccounts[idAccount].columna5.substring(0, 1) == paramPUC) {
                                        arrCuatroDigitos.push(arrQuiebre);
                                    }
                                }

                            } else {
                                arrCuatroDigitos.push(arrQuiebre);
                            }
                        }
                    }

                    intDMinReg = intDMaxReg;
                    intDMaxReg += 1000;

                    if (intLength < 1000) {
                        DbolStop = true;
                    }
                } else {
                    DbolStop = true;
                }
            }
            if (paramDigits == 2) {
                return arrSeisDigitos;
            } else if (paramDigits == 3) {
                return arrOchoDigitos;
            } else {
                return arrCuatroDigitos;
            }
        }

        function agruparNivel6(arrData) {
            var resultTot = new Array();
            var importe = 0;

            for (var i = 0; i < arrData.length; i++) {

                if (i == 0) {
                    importe = Number(arrData[i][3]);
                }

                if (i == arrData.length - 1) {
                    var result = new Array();
                    result.push(arrData[i][0].substring(0, 6)); //puc
                    result.push(arrData[i][5]); //DESCRIPCION
                    result.push(importe); //importe
                    result.push(arrData[i][6]); //decripcion puc 4d
                    result.push(arrData[i][7]); //decripcion puc 2d
                    result.push(arrData[i][8]); //decripcion puc 1d
                    resultTot.push(result);

                } else {
                    if (arrData[i][0].substring(0, 6) == arrData[i + 1][0].substring(0, 6)) {
                        importe += Number(arrData[i + 1][3]);
                        importe = redondear(importe);

                    } else {
                        var result = new Array();
                        result.push(arrData[i][0].substring(0, 6)); //puc
                        result.push(arrData[i][5]); //DESCRIPCION
                        result.push(importe); //importe
                        result.push(arrData[i][6]); //decripcion puc 4d
                        result.push(arrData[i][7]); //decripcion puc 2d
                        result.push(arrData[i][8]); //decripcion puc 1d
                        resultTot.push(result);
                        importe = Number(arrData[i + 1][3]);
                    }
                }
            }
            return resultTot;
        }


        function agruparNivel4(arrData) {
            var resultTot = new Array();
            var importe = 0;

            for (var i = 0; i < arrData.length; i++) {

                if (i == 0) {
                    if (paramDigits == 3) {
                        importe = Number(arrData[i][2]);
                    } else {
                        importe = Number(arrData[i][3]);
                    }

                }

                if (i == arrData.length - 1) {
                    var result = new Array();
                    if (paramDigits == 3) {
                        result.push(arrData[i][0].substring(0, 4)); //puc
                        result.push(arrData[i][3]); //DESCRIPCION
                        result.push(importe); //importe
                        result.push(arrData[i][4]); //decripcion puc 2d
                        result.push(arrData[i][5]); //decripcion puc 1d
                    } else {
                        result.push(arrData[i][0].substring(0, 4)); //puc
                        result.push(arrData[i][5]); //DESCRIPCION
                        result.push(importe); //importe
                        result.push(arrData[i][6]); //decripcion puc 2d
                        result.push(arrData[i][7]); //decripcion puc 1d
                    }
                    resultTot.push(result);

                } else {
                    if (arrData[i][0].substring(0, 4) == arrData[i + 1][0].substring(0, 4)) {
                        if (paramDigits == 3) {
                            importe += Number(arrData[i + 1][2]);
                        } else {
                            importe += Number(arrData[i + 1][3]);
                        }
                        importe = redondear(importe);

                    } else {
                        var result = new Array();
                        if (paramDigits == 3) {
                            result.push(arrData[i][0].substring(0, 4)); //puc
                            result.push(arrData[i][3]); //DESCRIPCION
                            result.push(importe); //importe
                            result.push(arrData[i][4]); //decripcion puc 2d
                            result.push(arrData[i][5]); //decripcion puc 1d
                        } else {
                            result.push(arrData[i][0].substring(0, 4)); //puc
                            result.push(arrData[i][5]); //DESCRIPCION
                            result.push(importe); //importe
                            result.push(arrData[i][6]); //decripcion puc 2d
                            result.push(arrData[i][7]); //decripcion puc 1d
                        }
                        resultTot.push(result);
                        if (paramDigits == 3) {
                            importe = Number(arrData[i + 1][2]);
                        } else {
                            importe = Number(arrData[i + 1][3]);
                        }
                    }
                }
            }

            return resultTot;
        }

        function agruparNivel2(arrData) {
            var resultTot = new Array();
            var importe = 0;

            for (var i = 0; i < arrData.length; i++) {

                if (i == 0) {
                    if (paramDigits == 2) {
                        importe = Number(arrData[i][2]);
                    } else if (paramDigits == 3) {
                        importe = Number(arrData[i][2]);
                    } else {
                        importe = Number(arrData[i][3]);
                    }

                }

                if (i == arrData.length - 1) {
                    var result = new Array();
                    if (paramDigits == 2) {
                        result.push(arrData[i][0].substring(0, 2)); //puc
                        result.push(arrData[i][3]); //DESCRIPCION
                        result.push(importe); //importe
                        result.push(arrData[i][4]); //decripcion puc 1d
                    } else if (paramDigits == 3) {
                        result.push(arrData[i][0].substring(0, 2)); //puc
                        result.push(arrData[i][3]); //DESCRIPCION
                        result.push(importe); //importe
                        result.push(arrData[i][4]); //decripcion puc 1d
                    } else {
                        result.push(arrData[i][0].substring(0, 2)); //puc
                        result.push(arrData[i][5]); //DESCRIPCION
                        result.push(importe); //importe
                        result.push(arrData[i][6]); //decripcion puc 1d
                    }
                    resultTot.push(result);

                } else {
                    if (arrData[i][0].substring(0, 2) == arrData[i + 1][0].substring(0, 2)) {
                        if (paramDigits == 2) {
                            importe += Number(arrData[i + 1][2]);
                        } else if (paramDigits == 3) {
                            importe += Number(arrData[i + 1][2]);
                        } else {
                            importe += Number(arrData[i + 1][3]);
                        }
                        importe = redondear(importe);

                    } else {
                        var result = new Array();

                        if (paramDigits == 2) {
                            result.push(arrData[i][0].substring(0, 2)); //puc
                            result.push(arrData[i][3]); //DESCRIPCION
                            result.push(importe); //importe
                            result.push(arrData[i][4]); //decripcion puc 1d
                        } else if (paramDigits == 3) {
                            result.push(arrData[i][0].substring(0, 2)); //puc
                            result.push(arrData[i][3]); //DESCRIPCION
                            result.push(importe); //importe
                            result.push(arrData[i][4]); //decripcion puc 1d
                        } else {
                            result.push(arrData[i][0].substring(0, 2)); //puc
                            result.push(arrData[i][5]); //DESCRIPCION
                            result.push(importe); //importe
                            result.push(arrData[i][6]); //decripcion puc 1d
                        }

                        resultTot.push(result);
                        if (paramDigits == 2) {
                            importe = Number(arrData[i + 1][2]);
                        } else if (paramDigits == 3) {
                            importe = Number(arrData[i + 1][2]);
                        } else {
                            importe = Number(arrData[i + 1][3]);
                        }

                    }
                }
            }

            return resultTot;
        }

        function agruparNivel1(arrData) {
            var resultTot = new Array();
            var importe = 0;

            for (var i = 0; i < arrData.length; i++) {

                if (i == 0) {
                    importe = Number(arrData[i][2]);
                }

                if (i == arrData.length - 1) {
                    var result = new Array();
                    result.push(arrData[i][0].substring(0, 1)); //puc
                    result.push(arrData[i][3]); //DESCRIPCION
                    result.push(importe); //importe
                    resultTot.push(result);

                } else {
                    if (arrData[i][0].substring(0, 1) == arrData[i + 1][0].substring(0, 1)) {
                        importe += Number(arrData[i + 1][2]);
                        importe = redondear(importe);

                    } else {
                        var result = new Array();
                        result.push(arrData[i][0].substring(0, 1)); //puc
                        result.push(arrData[i][3]); //DESCRIPCION
                        result.push(importe); //importe
                        resultTot.push(result);
                        importe = Number(arrData[i + 1][2]);

                    }
                }
            }

            return resultTot;
        }

        function validateSplitLine(descripcion) {
            var regex = /\r\n/gi;
            var resultDescripcion = descripcion.replace(regex, ' ')
            return resultDescripcion;
        }

        function obtenerDescripPUC() {
            var intDMinReg = 0;
            var intDMaxReg = 1000;
            var DbolStop = false;
            var ArrReturn = new Array();

            var busqueda = search.create({
                type: "customrecord_lmry_co_puc",
                filters: [
                    ["isinactive", "is", "F"],
                    "AND", ["formulatext: {custrecord_lmry_co_puc_subacc_of_digit1.name}", "is", paramPUC]
                ],
                columns: [
                    search.createColumn({
                        name: "name",
                        sort: search.Sort.ASC,
                        label: "0. PUC 4D"
                    }),
                    search.createColumn({
                        name: "custrecord_lmry_co_puc",
                        label: "1. DESCRIP. PUC 4D"
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "{custrecord_lmry_co_puc_subacc_of_digit2.custrecord_lmry_co_puc}",
                        label: "2. DESCRIP PUC 2D"
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "{custrecord_lmry_co_puc_subacc_of_digit1.custrecord_lmry_co_puc}",
                        label: "3. DESCRIP PUC 1D"
                    })
                ]
            });


            if (paramDigits == 2) {
                var Length6Digitos = search.createFilter({
                    name: 'formulatext',
                    formula: 'LENGTH({name})',
                    operator: search.Operator.IS,
                    values: 6
                });
                busqueda.filters.push(Length6Digitos);

                // DESCRIPCION PUC 4D
                var descripcion4dColumn = search.createColumn({
                    name: "formulatext",
                    formula: "{custrecord_lmry_co_puc_subacc_of_digit4.custrecord_lmry_co_puc}",
                    label: "DESCRIP PUC 4D"
                });
                busqueda.columns.push(descripcion4dColumn);

            } else if (paramDigits == 3) {
                var Length8Digitos = search.createFilter({
                    name: 'formulatext',
                    formula: 'LENGTH({name})',
                    operator: search.Operator.IS,
                    values: 8
                });
                busqueda.filters.push(Length8Digitos);

                // DESCRIPCION PUC 6D
                var descripcion6dColumn = search.createColumn({
                    name: "formulatext",
                    formula: "{custrecord_lmry_co_puc_subacc_of_digit6.custrecord_lmry_co_puc}",
                    label: "DESCRIP PUC 6D"
                });
                busqueda.columns.push(descripcion6dColumn);

                // DESCRIPCION PUC 4D
                var descripcion4dColumn = search.createColumn({
                    name: "formulatext",
                    formula: "{custrecord_lmry_co_puc_subacc_of_digit4.custrecord_lmry_co_puc}",
                    label: "DESCRIP PUC 4D"
                });
                busqueda.columns.push(descripcion4dColumn);


            } else {
                var Length4Digitos = search.createFilter({
                    name: 'formulatext',
                    formula: 'LENGTH({name})',
                    operator: search.Operator.IS,
                    values: '4'
                });
                busqueda.filters.push(Length4Digitos);
            }

            var savedsearch = busqueda.run();

            while (!DbolStop) {
                var objResult = savedsearch.getRange(intDMinReg, intDMaxReg);
                if (objResult != null) {
                    if (objResult.length != 1000) {
                        DbolStop = true;
                    }

                    for (var i = 0; i < objResult.length; i++) {
                        var columns = objResult[i].columns;
                        var arrAuxiliar = new Array();
                        if (paramDigits == 2) {
                            //0. PUC 6D
                            if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '')
                                arrAuxiliar[0] = objResult[i].getValue(columns[0]);
                            else
                                arrAuxiliar[0] = '';
                            //1. DESCRIPCION PUC 6D
                            if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '')
                                arrAuxiliar[1] = validateSplitLine(objResult[i].getValue(columns[1]));
                            else
                                arrAuxiliar[1] = '';
                            //2. DESCRIPCION PUC 4D
                            if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '')
                                arrAuxiliar[2] = validateSplitLine(objResult[i].getValue(columns[4]));
                            else
                                arrAuxiliar[2] = '';
                            //3. DESCRIPCION PUC 2D
                            if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '')
                                arrAuxiliar[3] = validateSplitLine(objResult[i].getValue(columns[2]));
                            else
                                arrAuxiliar[3] = '';
                            //4. DESCRIPCION PUC 1D
                            if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '')
                                arrAuxiliar[4] = validateSplitLine(objResult[i].getValue(columns[3]));
                            else
                                arrAuxiliar[4] = '';

                            var jsonCoPuc = {
                                puc6id: arrAuxiliar[0],
                                puc6idDescription: arrAuxiliar[1],
                                puc4idDescription: arrAuxiliar[2],
                                puc2idDescription: arrAuxiliar[3],
                                puc1idDescription: arrAuxiliar[4],
                            }
                        } else if (paramDigits == 3) {
                            //0. PUC 8D
                            if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '')
                                arrAuxiliar[0] = objResult[i].getValue(columns[0]);
                            else
                                arrAuxiliar[0] = '';
                            //1. DESCRIPCION PUC 8D
                            if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '')
                                arrAuxiliar[1] = validateSplitLine(objResult[i].getValue(columns[1]));
                            else
                                arrAuxiliar[1] = '';
                            //2. DESCRIPCION PUC 6D
                            if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '')
                                arrAuxiliar[2] = validateSplitLine(objResult[i].getValue(columns[4]));
                            else
                                arrAuxiliar[2] = '';
                            //2. DESCRIPCION PUC 4D
                            if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '')
                                arrAuxiliar[3] = validateSplitLine(objResult[i].getValue(columns[5]));
                            else
                                arrAuxiliar[3] = '';
                            //4. DESCRIPCION PUC 2D
                            if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '')
                                arrAuxiliar[4] = validateSplitLine(objResult[i].getValue(columns[2]));
                            else
                                arrAuxiliar[4] = '';
                            //5. DESCRIPCION PUC 1D
                            if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '')
                                arrAuxiliar[5] = validateSplitLine(objResult[i].getValue(columns[3]));
                            else
                                arrAuxiliar[5] = '';

                            var jsonCoPuc = {
                                puc8id: arrAuxiliar[0],
                                puc8idDescription: arrAuxiliar[1],
                                puc6idDescription: arrAuxiliar[2],
                                puc4idDescription: arrAuxiliar[3],
                                puc2idDescription: arrAuxiliar[4],
                                puc1idDescription: arrAuxiliar[5],
                            }
                        } else {
                            //0. PUC 4D
                            if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '')
                                arrAuxiliar[0] = objResult[i].getValue(columns[0]);
                            else
                                arrAuxiliar[0] = '';
                            //1. DESCRIPCION PUC 4D
                            if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '')
                                arrAuxiliar[1] = validateSplitLine(objResult[i].getValue(columns[1]));
                            else
                                arrAuxiliar[1] = '';
                            //2. DESCRIPCION PUC 2D
                            if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '')
                                arrAuxiliar[2] = validateSplitLine(objResult[i].getValue(columns[2]));
                            else
                                arrAuxiliar[2] = '';
                            //3. DESCRIPCION PUC 1D
                            if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '')
                                arrAuxiliar[3] = validateSplitLine(objResult[i].getValue(columns[3]));
                            else
                                arrAuxiliar[3] = '';
                        }

                        ArrReturn.push(arrAuxiliar);

                    }

                    // var transactionFile = file.create({
                    //     name: 'jsonCoPuc.json',
                    //     fileType: file.Type.JSON,
                    //     contents: JSON.stringify(jsonCoPuc),
                    //     folder: 2104 //18966
                    // }).save();

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

        function llamarMapReduce() {
            var params = {};
            params['custscript_lmry_invbal_logid'] = paramLogId;
            params['custscript_lmry_invbal_periodo'] = paramPeriodo;
            params['custscript_lmry_invbal_fileid'] = paramFile;
            params['custscript_lmry_invbal_lastpuc'] = paramPUC;
            params['custscript_lmry_invbal_adjust'] = paramAdjustment;
            params['custscript_lmry_invbal_digits'] = paramDigits;
            params['custscript_lmry_invbal_periodo_rest'] = paramPeriodsRestantes;

            if (featSubsi) {
                params['custscript_lmry_invbal_subsi'] = paramSubsidi;
            }
            if (featMulti) {
                params['custscript_lmry_invbal_multibook'] = paramMulti;
            }

            var RedirecSchdl = task.create({
                taskType: task.TaskType.MAP_REDUCE,
                scriptId: 'customscript_lmry_co_inv_bal_mprd',
                deploymentId: 'customdeploy_lmry_co_inv_bal_mprd',
                params: params
            });
            // log.debug('llamando a map reduce para PUC', params);
            RedirecSchdl.submit();
        }

        function formatear(data) {
            var strTotal = '';
            for (var i = 0; i < data.length; i++) {
                strTotal += data[i].join('|') + '\r\n';
            }
            return strTotal;
        }

        function juntarSaldoYMovimiento(arrDataSaldo, arrDataMovimiento) {
            var arrTotal = arrDataSaldo;
            for (var i = 0; i < arrTotal.length; i++) {
                var json = matchPUC(arrTotal[i][0]);
                if (paramDigits == 2) {
                    // Agrega las descripciones a los arreglos ya obtenidos en cada posicion, agrega descripcion(6d,4d,2d,1d)
                    arrTotal[i].push(json.puc6);
                    arrTotal[i].push(json.puc4);
                    arrTotal[i].push(json.puc2);
                    arrTotal[i].push(json.puc1);
                } else if (paramDigits == 3) {
                    // Agrega las descripciones a los arreglos ya obtenidos en cada posicion, agrega descripcion(8d,6d,4d,2d,1d)
                    arrTotal[i].push(json.puc8);
                    arrTotal[i].push(json.puc6);
                    arrTotal[i].push(json.puc4);
                    arrTotal[i].push(json.puc2);
                    arrTotal[i].push(json.puc1);
                } else {
                    // Agrega las descripciones a los arreglos ya obtenidos en cada posicion, agrega descripcion(4d,2d,1d)
                    arrTotal[i].push(json.puc4);
                    arrTotal[i].push(json.puc2);
                    arrTotal[i].push(json.puc1);
                }

                var cant = arrDataMovimiento.length;
                var j = 0;
                while (j < cant) {
                    // Si el ID del movimiento es igual a un ID del total, sumas el monto
                    if (arrTotal[i][0] == arrDataMovimiento[j][0]) {
                        if (paramDigits == 2) {
                            arrTotal[i][3] = redondear(Number(arrTotal[i][3]) + Number(arrDataMovimiento[j][9]));
                        } else if (paramDigits == 3) {
                            arrTotal[i][3] = redondear(Number(arrTotal[i][3]) + Number(arrDataMovimiento[j][11]));
                        } else {
                            arrTotal[i][3] = redondear(Number(arrTotal[i][3]) + Number(arrDataMovimiento[j][7]));
                        }
                        // Elimina el arrDataMovimiento[j] 
                        arrDataMovimiento.splice(j, 1);
                        cant--;
                        break;
                    } else {
                        j++;
                    }
                }
            }
            //log.debug('arrTotal', arrTotal);
            //log.debug('arrDataMovimiento', arrDataMovimiento);
            // Los Movimientos que no son iguales al array total se van a agregar a ese array total
            for (var i = 0; i < arrDataMovimiento.length; i++) {
                var arrMovimientosN = new Array();
                arrMovimientosN.push(arrDataMovimiento[i][0]);
                arrMovimientosN.push('');
                arrMovimientosN.push('');
                if (paramDigits == 2) {
                    arrMovimientosN.push(Number(arrDataMovimiento[i][9]));
                } else if (paramDigits == 3) {
                    arrMovimientosN.push(Number(arrDataMovimiento[i][11]));
                } else {
                    arrMovimientosN.push(Number(arrDataMovimiento[i][7]));
                }
                var json = matchPUC(arrDataMovimiento[i][0]);
                if (paramDigits == 2) {
                    arrMovimientosN.push(json.puc6);
                    arrMovimientosN.push(json.puc4);
                    arrMovimientosN.push(json.puc2);
                    arrMovimientosN.push(json.puc1);
                } else if (paramDigits == 3) {
                    arrMovimientosN.push(json.puc8);
                    arrMovimientosN.push(json.puc6);
                    arrMovimientosN.push(json.puc4);
                    arrMovimientosN.push(json.puc2);
                    arrMovimientosN.push(json.puc1);
                } else {
                    arrMovimientosN.push(json.puc4);
                    arrMovimientosN.push(json.puc2);
                    arrMovimientosN.push(json.puc1);
                }

                arrTotal.push(arrMovimientosN);
            }
            arrTotal = OrdenarCuentas(arrTotal);
            return arrTotal;
        }

        function matchPUC(puc) {
            var i = 0;
            var cant = Pucs.length;
            if (paramDigits == 2) {
                var jsonData = {
                    puc1: '',
                    puc2: '',
                    puc4: '',
                    puc6: '',
                };
                while (i < cant) {
                    if (Pucs[i][0] == puc) {
                        // Obtiene las descripciones de Pucs ([1]=6digito,[2]=4digitos,[3]=2digito,[4]=1digito)
                        jsonData.puc6 = Pucs[i][1];
                        jsonData.puc4 = Pucs[i][2];
                        jsonData.puc2 = Pucs[i][3];
                        jsonData.puc1 = Pucs[i][4];
                        // Elimina el elemento Pucs[i]
                        Pucs.splice(i, 1);
                        break;
                    } else {
                        i++;
                    }
                }
            } else if (paramDigits == 3) {
                var jsonData = {
                    puc1: '',
                    puc2: '',
                    puc4: '',
                    puc6: '',
                    puc8: '',
                };
                while (i < cant) {
                    if (Pucs[i][0] == puc) {
                        // Obtiene las descripciones de Pucs ([1]=6digito,[2]=4digitos,[3]=2digito,[4]=1digito)
                        jsonData.puc8 = Pucs[i][1];
                        jsonData.puc6 = Pucs[i][2];
                        jsonData.puc4 = Pucs[i][3];
                        jsonData.puc2 = Pucs[i][4];
                        jsonData.puc1 = Pucs[i][5];
                        // Elimina el elemento Pucs[i]
                        Pucs.splice(i, 1);
                        break;
                    } else {
                        i++;
                    }
                }
            } else {
                var jsonData = {
                    puc1: '',
                    puc2: '',
                    puc4: '',
                };
                while (i < cant) {
                    if (Pucs[i][0] == puc) {
                        // Obtiene las descripciones de Pucs ([1]=4digito,[2]=2digitos,[3]=1digito)
                        jsonData.puc4 = Pucs[i][1];
                        jsonData.puc2 = Pucs[i][2];
                        jsonData.puc1 = Pucs[i][3];
                        // Elimina el elemento Pucs[i]
                        Pucs.splice(i, 1);
                        break;
                    } else {
                        i++;
                    }
                }
            }
            return jsonData;
        }

        function obtenerDataAnterior() {
            if (paramFile != '' && paramFile != null) {
                var fileObj = fileModulo.load({
                    id: paramFile
                });
                var lineas = fileObj.getContents()
                lineas = lineas.split('\r\n');

                for (var i = 0; i < lineas.length; i++) {
                    var detail = lineas[i].split('|');
                    //log.debug('detail',detail);
                    if (detail[0] != '') {
                        if (detail[0].charAt(0) == paramPUC) {
                            SaldoAnteriorPUC.push(detail);
                        } else {
                            SaldoAnterior.push(detail);
                        }
                    }

                }
                log.debug('SaldoAnteriorPUC', SaldoAnteriorPUC);
                log.debug('SaldoAnterior', SaldoAnterior);
            } else {
                // log.debug('Alerta', 'No existe parametro de archivo');
            }
        }

        function SetPUCMultibook(ArrTemp) {

            for (var i = 0; i < ArrTemp.length; i++) {
                if (paramDigits == 2) {
                    if (jsonAccounts[ArrTemp[i][15]] != null) {
                        ArrTemp[i][0] = jsonAccounts[ArrTemp[i][15]].columna7;
                        ArrTemp[i][1] = jsonAccounts[ArrTemp[i][15]].columna8;
                        ArrTemp[i][2] = jsonAccounts[ArrTemp[i][15]].columna5;
                        ArrTemp[i][3] = jsonAccounts[ArrTemp[i][15]].columna6;
                        ArrTemp[i][4] = jsonAccounts[ArrTemp[i][15]].columna3;
                        ArrTemp[i][5] = jsonAccounts[ArrTemp[i][15]].columna4;
                        ArrTemp[i][6] = jsonAccounts[ArrTemp[i][15]].columna1;
                        ArrTemp[i][7] = jsonAccounts[ArrTemp[i][15]].columna2;
                    }
                } else if (paramDigits == 3) {
                    if (jsonAccounts[ArrTemp[i][17]] != null) {
                        ArrTemp[i][0] = jsonAccounts[ArrTemp[i][17]].columna9;
                        ArrTemp[i][1] = jsonAccounts[ArrTemp[i][17]].columna10;
                        ArrTemp[i][2] = jsonAccounts[ArrTemp[i][17]].columna7;
                        ArrTemp[i][3] = jsonAccounts[ArrTemp[i][17]].columna8;
                        ArrTemp[i][4] = jsonAccounts[ArrTemp[i][17]].columna5;
                        ArrTemp[i][5] = jsonAccounts[ArrTemp[i][17]].columna6;
                        ArrTemp[i][6] = jsonAccounts[ArrTemp[i][17]].columna3;
                        ArrTemp[i][7] = jsonAccounts[ArrTemp[i][17]].columna4;
                        ArrTemp[i][8] = jsonAccounts[ArrTemp[i][17]].columna1;
                        ArrTemp[i][9] = jsonAccounts[ArrTemp[i][17]].columna2;
                    }
                } else {
                    if (jsonAccounts[ArrTemp[i][13]] != null) {
                        ArrTemp[i][0] = jsonAccounts[ArrTemp[i][13]].columna5;
                        ArrTemp[i][1] = jsonAccounts[ArrTemp[i][13]].columna6;
                        ArrTemp[i][2] = jsonAccounts[ArrTemp[i][13]].columna3;
                        ArrTemp[i][3] = jsonAccounts[ArrTemp[i][13]].columna4;
                        ArrTemp[i][4] = jsonAccounts[ArrTemp[i][13]].columna1;
                        ArrTemp[i][5] = jsonAccounts[ArrTemp[i][13]].columna2;
                    }
                }
            }
            return ArrTemp;
        }


        function ObtenerCuentas() {
            var intDMinReg = 0;
            var intDMaxReg = 1000;
            var DbolStop = false;
            var jsonReturn = {};

            var busqueda = search.create({
                type: search.Type.ACCOUNT,
                filters: [],
                columns: ['internalid', 'number', 'custrecord_lmry_co_puc_d1_id', 'custrecord_lmry_co_puc_d1_description', 'custrecord_lmry_co_puc_d2_id', 'custrecord_lmry_co_puc_d2_description', 'custrecord_lmry_co_puc_d4_id', 'custrecord_lmry_co_puc_d4_description', 'custrecord_lmry_co_puc_d6_id', 'custrecord_lmry_co_puc_d6_description', 'type']
            });

            // Para filtrar que la cuenta tengan PUC ID 4D
            var filterIsNotEmptyPUCd4 = search.createFilter({
                name: "formulatext",
                formula: "{custrecord_lmry_co_puc_d4_id}",
                operator: search.Operator.ISNOTEMPTY,
                values: ""
            });
            busqueda.filters.push(filterIsNotEmptyPUCd4);

            // 10. PARA 8 DIGITOS
            if (paramDigits == 3) {
                var ochoDigitosColumn = search.createColumn({
                    name: 'formulatext',
                    formula: "{custrecord_lmry_co_puc_id.custrecord_lmry_co_puc_subacc_of_digit8}"
                });
                busqueda.columns.push(ochoDigitosColumn);
            }

            var savedsearch = busqueda.run();

            while (!DbolStop) {
                var objResult = savedsearch.getRange(intDMinReg, intDMaxReg);

                if (objResult != null) {
                    if (objResult.length != 1000) {
                        DbolStop = true;
                    }

                    for (var i = 0; i < objResult.length; i++) {
                        var columns = objResult[i].columns;
                        var internalid = objResult[i].getValue(columns[0]);
                        if (paramDigits == 3) {
                            // Para obtener la descripcion
                            var descript8Digit = '';
                            if (objResult[i].getValue(columns[11]) != null && objResult[i].getValue(columns[11]) != '' && objResult[i].getValue(columns[11]) != "-None-") {
                                descript8Digit = jsonDescriptionPuc8[objResult[i].getValue(columns[11])] || '';
                            } else {
                                descript8Digit = '';
                            }

                            jsonReturn[internalid] = {
                                columna0: objResult[i].getValue(columns[1]) || '', //number
                                columna1: objResult[i].getText(columns[2]) || '', //puc 1 id
                                columna2: objResult[i].getValue(columns[3]) || '', //puc 1 des
                                columna3: objResult[i].getText(columns[4]) || '', //puc 2 id
                                columna4: objResult[i].getValue(columns[5]) || '', //puc 2 des
                                columna5: objResult[i].getText(columns[6]) || '', //puc 4 id
                                columna6: objResult[i].getValue(columns[7]) || '', //puc 4 des
                                columna7: objResult[i].getText(columns[8]) || '', //puc 6 id
                                columna8: objResult[i].getValue(columns[9]) || '', //puc 6 des
                                columna9: objResult[i].getValue(columns[11]) || '', //puc 8 id
                                columna10: descript8Digit || '', //puc 8 des
                                type: objResult[i].getText(columns[10]) || ''
                            }
                        } else {
                            jsonReturn[internalid] = {
                                columna0: objResult[i].getValue(columns[1]) || '', //number
                                columna1: objResult[i].getText(columns[2]) || '', //puc 1 id
                                columna2: objResult[i].getValue(columns[3]) || '', //puc 1 des
                                columna3: objResult[i].getText(columns[4]) || '', //puc 2 id
                                columna4: objResult[i].getValue(columns[5]) || '', //puc 2 des
                                columna5: objResult[i].getText(columns[6]) || '', //puc 4 id
                                columna6: objResult[i].getValue(columns[7]) || '', //puc 4 des
                                columna7: objResult[i].getText(columns[8]) || '', //puc 6 id
                                columna8: objResult[i].getValue(columns[9]) || '', //puc 6 des
                                type: objResult[i].getText(columns[10]) || ''
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
            return jsonReturn;
        }

        function AgruparCuentas(ArrTemp) {
            var ArrReturn = new Array();
            ArrReturn.push(ArrTemp[0]);
            for (var i = 1; i < ArrTemp.length; i++) {
                if (ArrTemp[i][0].trim() != '') {
                    var intLength = ArrReturn.length;

                    for (var j = 0; j < intLength; j++) {

                        if (ArrReturn[j][0] == ArrTemp[i][0]) {

                            if (paramDigits == 2) {
                                ArrReturn[j][9] = redondear(ArrReturn[j][9] + ArrTemp[i][9]);
                            } else if (paramDigits == 3) {
                                ArrReturn[j][11] = redondear(ArrReturn[j][11] + ArrTemp[i][11]);
                            } else {
                                ArrReturn[j][7] = redondear(ArrReturn[j][7] + ArrTemp[i][7]);
                            }
                            break;
                        }
                        if (j == ArrReturn.length - 1) {
                            // Recorrer array agrupado completo y al no encontrar lo pushea el arreglo con diferente ID
                            ArrReturn.push(ArrTemp[i]);
                        }
                    }
                }
            }

            // Colocar montos 0 a lineas con decimales exageradamente pequeños
            for (var iter = 0; iter < ArrReturn.length; iter++) {
                if (paramDigits == 2) {
                    var montoRedondeado = Math.abs(redondear(ArrReturn[iter][9]));
                    if (montoRedondeado == 0) {
                        ArrReturn[iter][9] = 0;
                    }
                } else if (paramDigits == 3) {
                    var montoRedondeado = Math.abs(redondear(ArrReturn[iter][11]));
                    if (montoRedondeado == 0) {
                        ArrReturn[iter][11] = 0;
                    }
                } else {
                    var montoRedondeado = Math.abs(redondear(ArrReturn[iter][7]));
                    if (montoRedondeado == 0) {
                        ArrReturn[iter][7] = 0;
                    }
                }
            }
            return ArrReturn;
        }


        function GenerarExcel8Digitos(arr8D, arr6D, arr4D, arr2D, arr1D) {
            //Obtengo el total de Lineas a imprimir
            //var nLinea = 3000;
            xlsString = '<?xml version="1.0" encoding="UTF-8" ?><?mso-application progid="Excel.Sheet"?>';
            xlsString += '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" ';
            xlsString += 'xmlns:o="urn:schemas-microsoft-com:office:office" ';
            xlsString += 'xmlns:x="urn:schemas-microsoft-com:office:excel" ';
            xlsString += 'xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet" ';
            xlsString += 'xmlns:html="http://www.w3.org/TR/REC-html40">';
            xlsString += '<Styles>';
            xlsString += '<Style ss:ID="s21"><Font ss:Bold="1" ss:Size="12" /><Alignment ss:Horizontal="Center" ss:Vertical="Bottom"/></Style>';
            xlsString += '<Style ss:ID="s22"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom"/></Style>';
            xlsString += '<Style ss:ID="s23"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom"/><NumberFormat ss:Format="_(* #,###.00_);_(* \(#,###.00\);_(* &quot;-&quot;??_);_(@_)"/></Style>';
            xlsString += '<Style ss:ID="s24"><NumberFormat ss:Format="_(* #,###.00_);_(* \(#,##0.00\);_(* &quot;-&quot;??_);_(@_)"/></Style>';
            xlsString += '</Styles><Worksheet ss:Name="Sheet1">';

            xlsString += '<Table>';
            xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
            xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
            xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
            xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';

            //Cabecera
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['titulo'][language] + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row></Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            // nlapiLogExecution('ERROR', 'paramPeriodo-> ', paramPeriodo);

            xlsString += '<Cell  ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS['razonSocial'][language] + ': ' + companyname + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell  ss:StyleID="s22"><Data ss:Type="String">NIT: ' + companyruc + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell  ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS['corteAl'][language] + ': ' + Fecha_Corte_al + '</Data></Cell>';
            xlsString += '</Row>';
            if ((featMulti || featMulti == 'T') && (paramMulti != '' && paramMulti != null)) {
                xlsString += '<Row>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">Multibooking: ' + multibookName + '</Data></Cell>';
                xlsString += '</Row>';
            }
            
            //PDF Normalized
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["origin"][language] + "Netsuite" + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["date"][language] + todays + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["time"][language] + currentTime + '</Data></Cell>';
            xlsString += '</Row>';
            //PDF Normalized End

            xlsString += '<Row></Row>';
            xlsString += '<Row></Row>';
            xlsString += '<Row>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['cuenta'][language] + '</Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['denominacion'][language] + '</Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['importe'][language] + '</Data></Cell>' +
                '</Row>';

            var j = 0;
            var k = 0;
            var l = 0;
            var m = 0;

            for (var i = 0; i < arr1D.length; i++) {
                xlsString += '<Row>';
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr1D[i][0] + '</Data></Cell>';
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr1D[i][1] + '</Data></Cell>';
                xlsString += '<Cell ss:StyleID="s23"><Data ss:Type="Number">' + arr1D[i][2] + '</Data></Cell>';
                xlsString += '</Row>';

                while (j < arr2D.length) {
                    if (arr2D[j][0].substring(0, 1) == arr1D[i][0]) {
                        xlsString += '<Row>';
                        xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr2D[j][0] + '</Data></Cell>';
                        xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr2D[j][1] + '</Data></Cell>';
                        xlsString += '<Cell ss:StyleID="s23"><Data ss:Type="Number">' + arr2D[j][2] + '</Data></Cell>';
                        xlsString += '</Row>';

                        while (k < arr4D.length) {
                            if (arr4D[k][0].substring(0, 2) == arr2D[j][0]) {
                                xlsString += '<Row>';
                                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr4D[k][0] + '</Data></Cell>';
                                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr4D[k][1] + '</Data></Cell>';
                                xlsString += '<Cell ss:StyleID="s23"><Data ss:Type="Number">' + arr4D[k][2] + '</Data></Cell>';
                                xlsString += '</Row>';

                                while (l < arr6D.length) {
                                    if (arr6D[l][0].substring(0, 4) == arr4D[k][0]) {
                                        xlsString += '<Row>';
                                        xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr6D[l][0] + '</Data></Cell>';
                                        xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr6D[l][1] + '</Data></Cell>';
                                        xlsString += '<Cell ss:StyleID="s23"><Data ss:Type="Number">' + arr6D[l][2] + '</Data></Cell>';
                                        xlsString += '</Row>';

                                        while (m < arr8D.length) {
                                            if (arr8D[m][0].substring(0, 6) == arr6D[l][0]) {
                                                xlsString += '<Row>';
                                                xlsString += '<Cell><Data ss:Type="String">' + arr8D[m][0] + '</Data></Cell>';
                                                xlsString += '<Cell><Data ss:Type="String">' + arr8D[m][4] + '</Data></Cell>';
                                                xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + arr8D[m][3] + '</Data></Cell>';
                                                xlsString += '</Row>';
                                                m++;
                                            } else {
                                                break;
                                            }
                                        }

                                        l++;
                                    } else {
                                        break;
                                    }
                                }
                                k++;
                            } else {
                                break;
                            }
                        }
                        j++;
                    } else {
                        break;
                    }
                }
            }

            // CAMBIO 2016/04/14 - FILA DIFERENCIA
            // Operacion con las Cuentas de 1 Digito (ACTIVOS + GASTOS - INGRESOS - PASIVO - PATRIMONIO)
            var montoTotal1Dig = arr1D.reduce(function (contador, e) {
                return redondear(contador + e[2]);
            }, 0)
            var montoTotal2Dig = arr2D.reduce(function (contador, e) {
                return redondear(contador + e[2]);
            }, 0)

            xlsString += '<Row>';
            //xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell  ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS['diferencia'][language] + '</Data></Cell>';
            xlsString += '<Cell ss:StyleID="s23"><Data ss:Type="Number">' + (montoTotal1Dig - montoTotal2Dig) + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '</Table></Worksheet></Workbook>';

            // Se arma el archivo EXCEL
            Final_string = encode.convert({
                string: xlsString,
                inputEncoding: encode.Encoding.UTF_8,
                outputEncoding: encode.Encoding.BASE_64
            });

            var nameFile = Name_File();
            saveFile(Final_string, nameFile, 'xls');
        }


        function GenerarExcel6Digitos(arr6D, arr4D, arr2D, arr1D) {

            // CONVEERTIR ARR6D EN JSONPUC 6
            var jsonPuc6 = {};

            for (var m = 0; m < arr6D.length; m++) {
                var pucPadre = arr6D[m][0].substring(0, 4);
                if (jsonPuc6[pucPadre] == undefined) {
                    jsonPuc6[pucPadre] = [arr6D[m]];
                } else {
                    jsonPuc6[pucPadre].push(arr6D[m]);
                }
            }

            //log.debug('jsonPuc6', jsonPuc6);
            //Obtengo el total de Lineas a imprimir
            //var nLinea = 3000;
            xlsString = '<?xml version="1.0" encoding="UTF-8" ?><?mso-application progid="Excel.Sheet"?>';
            xlsString += '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" ';
            xlsString += 'xmlns:o="urn:schemas-microsoft-com:office:office" ';
            xlsString += 'xmlns:x="urn:schemas-microsoft-com:office:excel" ';
            xlsString += 'xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet" ';
            xlsString += 'xmlns:html="http://www.w3.org/TR/REC-html40">';
            xlsString += '<Styles>';
            xlsString += '<Style ss:ID="s21"><Font ss:Bold="1" ss:Size="12" /><Alignment ss:Horizontal="Center" ss:Vertical="Bottom"/></Style>';
            xlsString += '<Style ss:ID="s22"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom"/></Style>';
            xlsString += '<Style ss:ID="s23"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom"/><NumberFormat ss:Format="_(* #,###.00_);_(* \(#,###.00\);_(* &quot;-&quot;??_);_(@_)"/></Style>';
            xlsString += '<Style ss:ID="s24"><NumberFormat ss:Format="_(* #,###.00_);_(* \(#,##0.00\);_(* &quot;-&quot;??_);_(@_)"/></Style>';
            xlsString += '</Styles><Worksheet ss:Name="Sheet1">';

            xlsString += '<Table>';
            xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
            xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
            xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
            xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';

            //Cabecera
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['titulo'][language] + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row></Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            // nlapiLogExecution('ERROR', 'paramPeriodo-> ', paramPeriodo);

            xlsString += '<Cell  ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS['razonSocial'][language] + ': ' + companyname + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell  ss:StyleID="s22"><Data ss:Type="String">NIT: ' + companyruc + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell  ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS['corteAl'][language] + ': ' + Fecha_Corte_al + '</Data></Cell>';
            xlsString += '</Row>';
            if ((featMulti || featMulti == 'T') && (paramMulti != '' && paramMulti != null)) {
                xlsString += '<Row>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">Multibooking: ' + multibookName + '</Data></Cell>';
                xlsString += '</Row>';
            }
            
            //PDF Normalized
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["origin"][language] + "Netsuite" + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["date"][language] + todays + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["time"][language] + currentTime + '</Data></Cell>';
            xlsString += '</Row>';
            //PDF Normalized End

            xlsString += '<Row></Row>';
            xlsString += '<Row></Row>';
            xlsString += '<Row>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['cuenta'][language] + '</Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['denominacion'][language] + '</Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['importe'][language] + '</Data></Cell>' +
                '</Row>';

            var j = 0;
            var k = 0;
            var l = 0;
            for (var i = 0; i < arr1D.length; i++) {
                xlsString += '<Row>';
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr1D[i][0] + '</Data></Cell>';
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr1D[i][1] + '</Data></Cell>';
                xlsString += '<Cell ss:StyleID="s23"><Data ss:Type="Number">' + arr1D[i][2] + '</Data></Cell>';
                xlsString += '</Row>';

                while (j < arr2D.length) {
                    if (arr2D[j][0].substring(0, 1) == arr1D[i][0]) {
                        // log.debug('arr1D', arr2D[j]);
                        // log.debug('j', j);
                        xlsString += '<Row>';
                        xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr2D[j][0] + '</Data></Cell>';
                        xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr2D[j][1] + '</Data></Cell>';
                        xlsString += '<Cell ss:StyleID="s23"><Data ss:Type="Number">' + arr2D[j][2] + '</Data></Cell>';
                        xlsString += '</Row>';

                        while (k < arr4D.length) {
                            if (arr4D[k][0].substring(0, 2) == arr2D[j][0]) {
                                // log.debug('arr1D', arr4D[k]);
                                // log.debug('k', k);
                                xlsString += '<Row>';
                                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr4D[k][0] + '</Data></Cell>';
                                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr4D[k][1] + '</Data></Cell>';
                                xlsString += '<Cell ss:StyleID="s23"><Data ss:Type="Number">' + arr4D[k][2] + '</Data></Cell>';
                                xlsString += '</Row>';
                                if (jsonPuc6[arr4D[k][0]] != undefined) {
                                    var data_aux = [];
                                    data_aux = jsonPuc6[arr4D[k][0]];
                                    // log.debug('arr4D[k][0]', arr4D[k][0]);
                                    // log.debug('data_aux', data_aux);
                                    for (var x = 0; x < data_aux.length; x++) {
                                        xlsString += '<Row>';
                                        xlsString += '<Cell><Data ss:Type="String">' + data_aux[x][0] + '</Data></Cell>';
                                        xlsString += '<Cell><Data ss:Type="String">' + data_aux[x][4] + '</Data></Cell>';
                                        xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + data_aux[x][3] + '</Data></Cell>';
                                        xlsString += '</Row>';
                                    }
                                }
                                // while (l < arr6D.length) {
                                //     if (arr6D[l][0].substring(0, 4) == arr4D[k][0]) {
                                //         xlsString += '<Row>';
                                //         xlsString += '<Cell><Data ss:Type="String">' + arr6D[l][0] + '</Data></Cell>';
                                //         xlsString += '<Cell><Data ss:Type="String">' + arr6D[l][4] + '</Data></Cell>';
                                //         xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + arr6D[l][3] + '</Data></Cell>';
                                //         xlsString += '</Row>';
                                //         l++;
                                //     } else {
                                //         break;
                                //     }
                                // }
                                k++;
                            } else {
                                break;
                            }
                        }
                        j++;
                    } else {
                        break;
                    }
                }
            }

            // CAMBIO 2016/04/14 - FILA DIFERENCIA
            // Operacion con las Cuentas de 1 Digito (ACTIVOS + GASTOS - INGRESOS - PASIVO - PATRIMONIO)
            var montoTotal1Dig = arr1D.reduce(function (contador, e) {
                return redondear(contador + e[2]);
            }, 0)
            var montoTotal2Dig = arr2D.reduce(function (contador, e) {
                return redondear(contador + e[2]);
            }, 0)


            xlsString += '<Row>';
            //xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell  ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS['diferencia'][language] + '</Data></Cell>';
            xlsString += '<Cell ss:StyleID="s23"><Data ss:Type="Number">' + (montoTotal1Dig - montoTotal2Dig) + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '</Table></Worksheet></Workbook>';

            // Se arma el archivo EXCEL
            Final_string = encode.convert({
                string: xlsString,
                inputEncoding: encode.Encoding.UTF_8,
                outputEncoding: encode.Encoding.BASE_64
            });

            var nameFile = Name_File();
            saveFile(Final_string, nameFile, 'xls');
        }

        function getTemplate() {
            var aux = fileModulo.load("./CO_InvBalance_TemplatePDF.xml");
            return aux.getContents();
        }

        function generarPDF(auxArr0, auxArr1, auxArr2, auxArr3, auxArr4) {
            var arr0 = auxArr0;
            var arr1 = auxArr1;
            var arr4 = auxArr4;

            if (auxArr2.length != 0) {
                var arr2 = auxArr2;
            } else {
                var arr2 = [];
                for (var i = 0; i < auxArr4.length; i++) {
                    arr2.push([auxArr4[i][0], auxArr4[i][4], auxArr4[i][3]]);
                }
            }

            if (auxArr3.length != 0) {
                var arr3 = auxArr3;
            } else {
                var arr3 = [];
                for (var i = 0; i < auxArr4.length; i++) {
                    arr3.push([auxArr4[i][0], auxArr4[i][4], auxArr4[i][3]]);
                }
            }

            log.debug('Arr 0', arr0);
            log.debug('Arr 1', arr1);
            log.debug('Arr 2', arr2);
            log.debug('Arr 3', arr3);
            log.debug('Arr 4', arr4);

            var language = runtime.getCurrentScript().getParameter({
                name: 'LANGUAGE'
            }).substring(0, 2);

            var montoTotal1Dig = arr0.reduce(function (contador, e) {
                return redondear(contador + e[2]);
            }, 0)
            var montoTotal2Dig = arr1.reduce(function (contador, e) {
                return redondear(contador + e[2]);
            }, 0)


            var j = 0,
                k = 0,
                l = 0,
                m = 0;
            var arrTransaction = [];

            for (var i = 0; i < arr0.length; i++) {
                arrTransaction.push({
                    "colum1": arr0[i][0],
                    "colum2": arr0[i][1],
                    "colum3": arr0[i][2],
                });
                while (j < arr1.length) {
                    if (arr1[j][0].substring(0, 1) == arr0[i][0]) {
                        arrTransaction.push({
                            "colum1": arr1[j][0],
                            "colum2": arr1[j][1],
                            "colum3": arr1[j][2],
                        });
                        while (k < arr2.length) {
                            if (arr2[k][0].substring(0, 2) == arr1[j][0]) {
                                arrTransaction.push({
                                    "colum1": arr2[k][0],
                                    "colum2": arr2[k][1],
                                    "colum3": arr2[k][2],
                                });
                                while (l < arr3.length) {
                                    if (arr3[l][0].substring(0, 4) == arr2[k][0]) {
                                        arrTransaction.push({
                                            "colum1": arr3[l][0],
                                            "colum2": arr3[l][1],
                                            "colum3": arr3[l][2],
                                        });
                                        if (paramDigits == '3') {
                                            while (m < arr4.length) {
                                                if (arr4[m][0].substring(0, 6) == arr3[l][0]) {
                                                    arrTransaction.push({
                                                        "colum1": arr4[m][0],
                                                        "colum2": arr4[m][4],
                                                        "colum3": arr4[m][3]
                                                    });
                                                    m++;
                                                } else {
                                                    break;
                                                }
                                            }
                                        }
                                        l++;
                                    } else {
                                        break;
                                    }
                                }
                                k++;
                            } else {
                                break;
                            }
                        }
                        j++;
                    } else {
                        break;
                    }
                }
            }

            var JsonTraslate = {
                "colum1": GLOBAL_LABELS['cuenta'][language],
                "colum2": GLOBAL_LABELS['denominacion'][language],
                "colum3": GLOBAL_LABELS['importe'][language],
                "diferencia": GLOBAL_LABELS['diferencia'][language]
            }

            var jsonAxiliar = {
                "company": {
                    "title": GLOBAL_LABELS['titulo'][language],
                    "razon": xml.escape(GLOBAL_LABELS['razonSocial'][language] + ': ' + companyname),
                    "ruc": 'NIT: ' + companyruc,
                    "date": GLOBAL_LABELS['corteAl'][language] + ': ' + Fecha_Corte_al,
                    "total": (montoTotal1Dig - montoTotal2Dig),
                    "pdfStandard": {
                        "origin": GLOBAL_LABELS["origin"][language] + "Netsuite",
                        "todays": GLOBAL_LABELS["date"][language] + todays,
                        "currentTime": GLOBAL_LABELS["time"][language] + currentTime,
                        "page": GLOBAL_LABELS["page"][language],
                        "of": GLOBAL_LABELS["of"][language]
                    }
                },
                "traslate": JsonTraslate,
                "movements": arrTransaction
            }

            if (featMulti) {
                jsonAxiliar["company"].mlb = 'Multibooking:' + multibookName;
            } else {
                jsonAxiliar["company"].mlb = '';
            }
            log.debug('jsonAxiliar', jsonAxiliar);
            var renderer = render.create();

            renderer.templateContent = getTemplate();
            log.debug('renderer.templateContent', renderer.templateContent);
            renderer.addCustomDataSource({
                format: render.DataSource.OBJECT,
                alias: "input",
                data: {
                    data: JSON.stringify(jsonAxiliar)
                }
            });



            /*** */
            stringXML = renderer.renderAsPdf();
            saveFilePDF(stringXML);
        }

        function saveFilePDF(stringXML) {
            var fileAuxliar = stringXML;
            var nameReport = Name_File() + '.pdf';
            var folderID = objContext.getParameter({
                name: 'custscript_lmry_file_cabinet_rg_co'
            });

            if (folderID != '' && folderID != null) {

                fileAuxliar.name = nameReport;
                fileAuxliar.folder = folderID;

                // Termina de grabar el archivo
                var idfile = fileAuxliar.save();
                // log.debug('Se actualizo archivo temporal con id: ', idfile);
                // Trae URL de archivo generado

                var idfile2 = fileModulo.load({
                    id: idfile
                });
                // Obtenemos de las prefencias generales el URL de Netsuite (Produccion o Sandbox)
                var getURL = objContext.getParameter({
                    name: 'custscript_lmry_netsuite_location'
                });

                var urlfile = '';
                if (getURL != '' && getURL != '') {
                    urlfile += 'https://' + getURL;
                }
                urlfile += idfile2.url;

                //Genera registro personalizado como log
                if (idfile) {
                    var usuarioTemp = runtime.getCurrentUser();
                    var id = usuarioTemp.id;
                    var employeename = search.lookupFields({
                        type: search.Type.EMPLOYEE,
                        id: id,
                        columns: ['firstname', 'lastname']
                    });
                    var usuario = employeename.firstname + ' ' + employeename.lastname;

                    var accountingName = search.lookupFields({
                        type: search.Type.ACCOUNTING_PERIOD,
                        id: jsonParametros.paramPeriodo,
                        columns: ['periodname']
                    });
                    var periodNamePDF = accountingName.periodname

                    var record = recordModulo.create({
                        type: 'customrecord_lmry_co_rpt_generator_log'
                    });
                    //Nombre de Archivo
                    record.setValue({
                        fieldId: 'custrecord_lmry_co_rg_name',
                        value: nameReport
                    });
                    //Nombre de Reporte
                    record.setValue({
                        fieldId: 'custrecord_lmry_co_rg_transaction',
                        value: 'CO - Libro de Inventario y Balance 2.0'
                    });
                    //Periodo
                    record.setValue({
                        fieldId: 'custrecord_lmry_co_rg_postingperiod',
                        value: periodNamePDF
                    });
                    //Nombre de Subsidiaria
                    record.setValue({
                        fieldId: 'custrecord_lmry_co_rg_subsidiary',
                        value: companyname
                    });
                    //Url de Archivo
                    record.setValue({
                        fieldId: 'custrecord_lmry_co_rg_url_file',
                        value: urlfile
                    });
                    //Multibook
                    if (featMulti || featMulti == 'T') {
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_rg_multibook',
                            value: multibookName
                        });
                    }
                    //Creado Por
                    record.setValue({
                        fieldId: 'custrecord_lmry_co_rg_employee',
                        value: usuario
                    });

                    var recordId = record.save();
                    libFeature.sendConfirmUserEmail(nameReport, 3, LMRY_script, language)
                }
            }
        }

        function GenerarExcel(arr4D, arr2D, arr1D) {
            //Obtengo el total de Lineas a imprimir
            //var nLinea = 3000;
            xlsString = '<?xml version="1.0" encoding="UTF-8" ?><?mso-application progid="Excel.Sheet"?>';
            xlsString += '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" ';
            xlsString += 'xmlns:o="urn:schemas-microsoft-com:office:office" ';
            xlsString += 'xmlns:x="urn:schemas-microsoft-com:office:excel" ';
            xlsString += 'xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet" ';
            xlsString += 'xmlns:html="http://www.w3.org/TR/REC-html40">';
            xlsString += '<Styles>';
            xlsString += '<Style ss:ID="s21"><Font ss:Bold="1" ss:Size="12" /><Alignment ss:Horizontal="Center" ss:Vertical="Bottom"/></Style>';
            xlsString += '<Style ss:ID="s22"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom"/></Style>';
            xlsString += '<Style ss:ID="s23"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom"/><NumberFormat ss:Format="_(* #,###.00_);_(* \(#,###.00\);_(* &quot;-&quot;??_);_(@_)"/></Style>';
            xlsString += '<Style ss:ID="s24"><NumberFormat ss:Format="_(* #,###.00_);_(* \(#,##0.00\);_(* &quot;-&quot;??_);_(@_)"/></Style>';
            xlsString += '</Styles><Worksheet ss:Name="Sheet1">';

            xlsString += '<Table>';
            xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
            xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
            xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
            xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';

            //Cabecera
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['titulo'][language] + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row></Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            // nlapiLogExecution('ERROR', 'paramPeriodo-> ', paramPeriodo);

            xlsString += '<Cell  ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS['razonSocial'][language] + ': ' + companyname + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell  ss:StyleID="s22"><Data ss:Type="String">NIT: ' + companyruc + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell  ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS['corteAl'][language] + ': ' + Fecha_Corte_al + '</Data></Cell>';
            xlsString += '</Row>';
            if ((featMulti || featMulti == 'T') && (paramMulti != '' && paramMulti != null)) {
                xlsString += '<Row>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">Multibooking: ' + multibookName + '</Data></Cell>';
                xlsString += '</Row>';
            }

            //PDF Normalized
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["origin"][language] + "Netsuite" + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["date"][language] + todays + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS["time"][language] + currentTime + '</Data></Cell>';
            xlsString += '</Row>';
            //PDF Normalized End

            xlsString += '<Row></Row>';
            xlsString += '<Row></Row>';
            xlsString += '<Row>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['cuenta'][language] + '</Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['denominacion'][language] + '</Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['importe'][language] + '</Data></Cell>' +
                '</Row>';

            var j = 0;
            var k = 0;
            for (var i = 0; i < arr1D.length; i++) {
                xlsString += '<Row>';
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr1D[i][0] + '</Data></Cell>';
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr1D[i][1] + '</Data></Cell>';
                xlsString += '<Cell ss:StyleID="s23"><Data ss:Type="Number">' + arr1D[i][2] + '</Data></Cell>';
                xlsString += '</Row>';

                while (j < arr2D.length) {
                    if (arr2D[j][0].substring(0, 1) == arr1D[i][0]) {
                        xlsString += '<Row>';
                        xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr2D[j][0] + '</Data></Cell>';
                        xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr2D[j][1] + '</Data></Cell>';
                        xlsString += '<Cell ss:StyleID="s23"><Data ss:Type="Number">' + arr2D[j][2] + '</Data></Cell>';
                        xlsString += '</Row>';

                        while (k < arr4D.length) {
                            if (arr4D[k][0].substring(0, 2) == arr2D[j][0]) {
                                xlsString += '<Row>';
                                xlsString += '<Cell><Data ss:Type="String">' + arr4D[k][0] + '</Data></Cell>';
                                xlsString += '<Cell><Data ss:Type="String">' + arr4D[k][4] + '</Data></Cell>';
                                xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + arr4D[k][3] + '</Data></Cell>';
                                xlsString += '</Row>';
                                k++;
                            } else {
                                break;
                            }
                        }
                        j++;
                    } else {
                        break;
                    }
                }
            }

            // CAMBIO 2016/04/14 - FILA DIFERENCIA
            // Operacion con las Cuentas de 1 Digito (ACTIVOS + GASTOS - INGRESOS - PASIVO - PATRIMONIO)
            var montoTotal1Dig = arr1D.reduce(function (contador, e) {
                return redondear(contador + e[2]);
            }, 0)
            var montoTotal2Dig = arr2D.reduce(function (contador, e) {
                return redondear(contador + e[2]);
            }, 0)

            xlsString += '<Row>';
            //xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell  ss:StyleID="s22"><Data ss:Type="String">' + GLOBAL_LABELS['diferencia'][language] + '</Data></Cell>';
            xlsString += '<Cell ss:StyleID="s23"><Data ss:Type="Number">' + (montoTotal1Dig - montoTotal2Dig) + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '</Table></Worksheet></Workbook>';

            // Se arma el archivo EXCEL
            Final_string = encode.convert({
                string: xlsString,
                inputEncoding: encode.Encoding.UTF_8,
                outputEncoding: encode.Encoding.BASE_64
            });

            var nameFile = Name_File();
            saveFile(Final_string, nameFile, 'xls');
        }

        function ValidatePrimaryBook() {
            var accbook_check = search.lookupFields({
                type: search.Type.ACCOUNTING_BOOK,
                id: paramMulti,
                columns: ['isprimary']
            });

            return accbook_check.isprimary;
        }

        function redondear(number) {
            return Math.round(Number(number) * 100) / 100;
        }

        function ObtieneSpecificTransaction() {
            // Control de Memoria
            var intDMaxReg = 1000;
            var intDMinReg = 0;
            var arrQuiebre = new Array();
            // Exedio las unidades
            var DbolStop = false;
            var arrCuatroDigitos = new Array();
            var arrSeisDigitos = new Array();
            var arrOchoDigitos = new Array();
            var _contg = 0;
            //var _cont = 0;

            var savedsearch = search.load({
                /*LatamReady - CO Inventory Book and Balance L.EspecMulti*/
                id: 'customsearch_lmry_co_invent_balanc_liesp'
            });

            var pucFilter = search.createFilter({
                name: 'formulatext',
                formula: '{account.custrecord_lmry_co_puc_d4_id}',
                operator: search.Operator.STARTSWITH,
                values: [paramPUC]
            });
            savedsearch.filters.push(pucFilter);

            if (featSubsi) {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [paramSubsidi]
                });
                savedsearch.filters.push(subsidiaryFilter);
            }

            var periodosSTR = PeriodosRestantes.toString();
            var periodFilterFROM = search.createFilter({
                name: 'formulanumeric',
                formula: 'CASE WHEN {transaction.postingperiod.id} IN (' + periodosSTR + ') THEN 1 ELSE 0 END',
                operator: search.Operator.EQUALTO,
                values: [1]
            });
            savedsearch.filters.push(periodFilterFROM);

            var multibookFilter = search.createFilter({
                name: 'accountingbook',
                operator: search.Operator.IS,
                values: [paramMulti]
            });
            savedsearch.filters.push(multibookFilter);


            // PARA 6 DIGITOS
            if (paramDigits == 2 || paramDigits == 3) {

                if (paramDigits == 2) {
                    var filterIsNotEmptyPUCd6 = search.createFilter({
                        name: "formulatext",
                        formula: "{account.custrecord_lmry_co_puc_d6_id}",
                        operator: search.Operator.ISNOTEMPTY,
                        values: ""
                    });
                    savedsearch.filters.push(filterIsNotEmptyPUCd6);
                }

                // 11.
                var seisDigitID = search.createColumn({
                    name: "formulatext",
                    summary: "GROUP",
                    formula: "{account.custrecord_lmry_co_puc_d6_id}",
                    sort: search.Sort.ASC,
                });
                savedsearch.columns.push(seisDigitID);

                //12.
                var seisDigitDescripcion = search.createColumn({
                    name: "formulatext",
                    summary: "GROUP",
                    formula: "{account.custrecord_lmry_co_puc_d6_description}",
                    sort: search.Sort.ASC,
                });
                savedsearch.columns.push(seisDigitDescripcion);
            }

            if (paramDigits == 3) {
                // REVISAR SI FUNCIONA ESTO XXXXXXXXXXXXXXXXXXXXXXXXX
                // Validar que en campo puc id, deban haber 8 digitos
                var filterIsNotEmptyPUCd8 = search.createFilter({
                    name: "formulatext",
                    formula: "LENGTH({account.custrecord_lmry_co_puc_id})",
                    operator: search.Operator.IS,
                    values: 8
                });
                savedsearch.filters.push(filterIsNotEmptyPUCd8);

                //13.
                var ochoDigitID = search.createColumn({
                    name: "formulatext",
                    summary: "GROUP",
                    formula: "{account.custrecord_lmry_co_puc_id}",
                    sort: search.Sort.ASC,
                });
                savedsearch.columns.push(ochoDigitID);
            }

            var searchresult = savedsearch.run();

            while (!DbolStop) {
                var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

                if (objResult != null) {
                    var intLength = objResult.length;

                    if (intLength != 1000) {
                        DbolStop = true;
                    }

                    for (var i = 0; i < intLength; i++) {
                        var columns = objResult[i].columns;
                        arrQuiebre = new Array();

                        if (paramDigits == 2) {
                            for (var col = 0; col < columns.length - 2; col++) {
                                arrQuiebre[0] = objResult[i].getValue(columns[11]);
                                arrQuiebre[1] = objResult[i].getValue(columns[12]);
                                if (col == 8) {
                                    arrQuiebre[10] = objResult[i].getText(columns[col]);
                                } else if (col == 7) {
                                    arrQuiebre[col + 2] = redondear(objResult[i].getValue(columns[col]) * objResult[i].getValue(columns[10]));
                                } else {
                                    arrQuiebre[col + 2] = objResult[i].getValue(columns[col]);
                                }
                            }

                            if (arrQuiebre[9] != 0) {
                                arrSeisDigitos[_contg] = arrQuiebre;
                                _contg++;
                            }
                        } else if (paramDigits == 3) {
                            // PARA 8 DIGITOS
                            var descripcion8Digit = '';
                            // Obtener la descripcion de 8 digitos  
                            descripcion8Digit = jsonDescriptionPuc8[objResult[i].getValue(columns[13])];
                            arrQuiebre[0] = objResult[i].getValue(columns[13]);
                            arrQuiebre[1] = descripcion8Digit;
                            arrQuiebre[2] = objResult[i].getValue(columns[11]);
                            arrQuiebre[3] = objResult[i].getValue(columns[12]);
                            for (var col = 0; col < columns.length - 3; col++) {
                                if (col == 8) {
                                    arrQuiebre[12] = objResult[i].getText(columns[col]);
                                } else if (col == 7) {
                                    arrQuiebre[col + 4] = redondear(objResult[i].getValue(columns[col]) * objResult[i].getValue(columns[10]));
                                } else {
                                    arrQuiebre[col + 4] = objResult[i].getValue(columns[col]);
                                }
                            }

                            if (arrQuiebre[11] != 0) {
                                arrOchoDigitos[_contg] = arrQuiebre;
                                _contg++;
                            }
                        } else {
                            for (var col = 0; col < columns.length; col++) {
                                if (col == 8) {
                                    arrQuiebre[col] = objResult[i].getText(columns[col]);
                                } else if (col == 7) {
                                    arrQuiebre[col] = redondear(objResult[i].getValue(columns[col]) * objResult[i].getValue(columns[10]));
                                } else {
                                    arrQuiebre[col] = objResult[i].getValue(columns[col]);
                                }
                            }

                            if (arrQuiebre[7] != 0) {
                                arrCuatroDigitos[_contg] = arrQuiebre;
                                _contg++;
                            }
                        }

                    }

                    intDMinReg = intDMaxReg;
                    intDMaxReg += 1000;
                    if (intLength < 1000) {
                        DbolStop = true;
                    }
                } else {
                    DbolStop = true;
                }
            }
            if (paramDigits == 2) {
                return arrSeisDigitos;
            } else if (paramDigits == 3) {
                return arrOchoDigitos;
            } else {
                return arrCuatroDigitos;
            }
        }

        function ObtieneDataSet(periodoAux) {
            /* Query */
            var trQuery = query.create({
                type: query.Type.TRANSACTION,
            });

            /* Joins */
            var trQueryJoinLine = trQuery.autoJoin({
                fieldId: 'transactionlines',
            });

            var trQueryJoinAcc = trQueryJoinLine.autoJoin({
                fieldId: 'accountingimpact',
            });

            /* Conditions */

            var paramPUCstr = paramPUC + '';

            var conditionsAnd = [
                trQuery.createCondition({
                    fieldId: 'posting',
                    operator: query.Operator.IS,
                    values: [
                        true,
                    ],
                }),
                trQuery.createCondition({
                    fieldId: "voided",
                    operator: query.Operator.IS,
                    values: [false]
                }),
                trQueryJoinLine.createCondition({
                    fieldId: 'subsidiary',
                    operator: query.Operator.ANY_OF,
                    values: [
                        paramSubsidi,
                    ],
                }),
                trQuery.createCondition({
                    fieldId: 'postingperiod',
                    operator: query.Operator.ANY_OF,
                    values: periodoAux,
                }),
                trQuery.createCondition({
                    operator: query.Operator.EQUAL,
                    values: [
                        '8',
                    ],
                    formula: "LENGTH({transactionlines.accountingimpact.account.custrecord_lmry_co_puc_id#display})",
                    type: query.ReturnType.INTEGER,
                }),
                trQuery.createCondition({
                    operator: query.Operator.START_WITH,
                    values: [
                        paramPUCstr,
                    ],
                    formula: "{transactionlines.accountingimpact.account.custrecord_lmry_co_puc_d6_id#display}",
                    type: query.ReturnType.STRING,
                })

            ];

            if (featMulti) {
                var isbookspecificCondition = trQuery.createCondition({
                    fieldId: 'isbookspecific',
                    operator: query.Operator.IS,
                    values: [
                        false,
                    ],
                });
                conditionsAnd.push(isbookspecificCondition);

                var multiCondition = trQueryJoinAcc.createCondition({
                    fieldId: 'accountingbook',
                    operator: query.Operator.ANY_OF,
                    values: [
                        paramMulti,
                    ],
                });
                conditionsAnd.push(multiCondition);
            }

            trQuery.condition = trQuery.and(conditionsAnd);

            /* Columns */

            trQuery.columns = [
                trQueryJoinAcc.createColumn({
                    fieldId: 'account',
                    context: {
                        name: 'RAW',
                    },
                    alias: 'accountid',
                    groupBy: true
                }),
                trQuery.createColumn({
                    type: query.ReturnType.FLOAT,
                    formula: "NVL(TO_NUMBER({transactionlines.accountingimpact.debit}),0) - NVL(TO_NUMBER({transactionlines.accountingimpact.credit}),0)",
                    alias: "balance",
                    aggregate: query.Aggregate.SUM,
                })
            ];

            var pageData = trQuery.runPaged({
                pageSize: 1000
            });

            var arrTotal = new Array;

            //log.debug('pageData.pageRanges.length', pageData.pageRanges.length);

            for (var i = 0; i < pageData.pageRanges.length; i++) {
                var page = pageData.fetch(i);
                var results = page.data.asMappedResults();
                //console.log(results);
                for (var j = 0; j < results.length; j++) {
                    var aux_arr = new Array;

                    // 0 account ID
                    if (results[j].accountid != null && results[j].accountid != '- None -') {
                        var acc_id = results[j].accountid;
                    } else {
                        var acc_id = '';
                    }
                    // 1. SALDO
                    if (results[j].balance != null && results[j].balance != '- None -') {
                        var balance = redondear(results[j].balance);
                    } else {
                        var balance = 0.00;
                    }
                    if (paramDigits == 3) {
                        aux_arr[0] = jsonAccounts[acc_id].columna9; //puc 8
                        aux_arr[1] = jsonAccounts[acc_id].columna10; //puc 8 descripcion
                        aux_arr[2] = jsonAccounts[acc_id].columna7; //puc 6
                        aux_arr[3] = jsonAccounts[acc_id].columna8; //puc 6 descripcion
                        aux_arr[4] = jsonAccounts[acc_id].columna5; //puc 4
                        aux_arr[5] = jsonAccounts[acc_id].columna6; //puc 4 descripcion
                        aux_arr[6] = jsonAccounts[acc_id].columna3; //puc 2 id
                        aux_arr[7] = jsonAccounts[acc_id].columna4; //puc 2 descripcion
                        aux_arr[8] = jsonAccounts[acc_id].columna1; //puc 1 id
                        aux_arr[9] = jsonAccounts[acc_id].columna2; //puc descripcion
                        aux_arr[10] = ''; //Antes id del periodo
                        aux_arr[11] = balance;
                        aux_arr[12] = jsonAccounts[acc_id].type;
                        aux_arr[13] = jsonAccounts[acc_id].columna9; //number
                        aux_arr[14] = 'ex';
                        aux_arr[15] = 'ex';
                        aux_arr[16] = balance;
                        aux_arr[17] = acc_id;
                    } else {
                        aux_arr[0] = jsonAccounts[acc_id].columna7; //puc 6
                        aux_arr[1] = jsonAccounts[acc_id].columna8; //puc 6 descripcion
                        aux_arr[2] = jsonAccounts[acc_id].columna5; //puc 4
                        aux_arr[3] = jsonAccounts[acc_id].columna6; //puc 4 descripcion
                        aux_arr[4] = jsonAccounts[acc_id].columna3; //puc 2 id
                        aux_arr[5] = jsonAccounts[acc_id].columna4; //puc 2 descripcion
                        aux_arr[6] = jsonAccounts[acc_id].columna1; //puc 1 id
                        aux_arr[7] = jsonAccounts[acc_id].columna2; //puc descripcion
                        aux_arr[8] = ''; //Antes id del periodo
                        aux_arr[9] = balance;
                        aux_arr[10] = jsonAccounts[acc_id].type;
                        aux_arr[11] = jsonAccounts[acc_id].columna7; //number
                        aux_arr[12] = 'ex';
                        aux_arr[13] = 'ex';
                        aux_arr[14] = balance;
                        aux_arr[15] = acc_id;
                    }
                    arrTotal.push(aux_arr);
                }
            }
            return arrTotal;
        }

        function ObtieneTransacciones(period_aux) {
            // Control de Memoria
            var intDMaxReg = 1000;
            var intDMinReg = 0;
            // Exedio las unidades
            var DbolStop = false;
            var arrCuatroDigitos = new Array();
            var arrSeisDigitos = new Array();
            var arrOchoDigitos = new Array();
            var _contg = 0;

            var period_aux_ = period_aux.join();

            var savedsearch = search.load({
                /*LatamReady - CO Inventory Book and Balance with L.Espec*/
                id: 'customsearch_lmry_co_invent_balanc_trale'
            });

            if (featSubsi) {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [paramSubsidi]
                });
                savedsearch.filters.push(subsidiaryFilter);
            }

            var periodosSTR = PeriodosRestantes.toString();
            var periodFilterFROM = search.createFilter({
                name: 'formulanumeric',
                formula: 'CASE WHEN {postingperiod.id} IN (' + period_aux_ + ') THEN 1 ELSE 0 END',
                operator: search.Operator.EQUALTO,
                values: [1]
            });
            savedsearch.filters.push(periodFilterFROM);

            if (featMulti) {
                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [paramMulti]
                });
                savedsearch.filters.push(multibookFilter);

                var bookspecificFitler = search.createFilter({
                    name: 'bookspecifictransaction',
                    operator: search.Operator.IS,
                    values: ['F']
                });
                savedsearch.filters.push(bookspecificFitler);
                //11.
                var exchangerateColum = search.createColumn({
                    name: 'formulacurrency',
                    summary: "GROUP",
                    formula: "{accountingtransaction.exchangerate}"
                });
                savedsearch.columns.push(exchangerateColum);
                //12.
                var balanceColumn = search.createColumn({
                    name: 'formulacurrency',
                    summary: "SUM",
                    formula: "NVL({accountingtransaction.debitamount},0) - NVL({accountingtransaction.creditamount},0)"
                });
                savedsearch.columns.push(balanceColumn);
                //13.
                var multiAccountColumn = search.createColumn({
                    name: 'account',
                    join: 'accountingtransaction',
                    summary: "GROUP"
                });
                savedsearch.columns.push(multiAccountColumn);


                if (paramDigits == 2 || paramDigits == 3) {
                    // 14.
                    var seisDigitID = search.createColumn({
                        name: "formulatext",
                        summary: "GROUP",
                        formula: "{account.custrecord_lmry_co_puc_d6_id}",
                        sort: search.Sort.ASC,
                    });
                    savedsearch.columns.push(seisDigitID);

                    //15.
                    var seisDigitDescripcion = search.createColumn({
                        name: "formulatext",
                        summary: "GROUP",
                        formula: "{account.custrecord_lmry_co_puc_d6_description}",
                        sort: search.Sort.ASC,
                    });
                    savedsearch.columns.push(seisDigitDescripcion);
                }


                if (paramDigits == 3) {
                    // Consultar si obligatoriamente estara con 8 digitos el LATAM CO PUC, para quitar esta validacion que tenga 8 digitos ese latam co puc
                    var Length8Digitos = search.createFilter({
                        name: 'formulatext',
                        formula: 'LENGTH({account.custrecord_lmry_co_puc_id})',
                        operator: search.Operator.IS,
                        values: 8
                    });
                    savedsearch.filters.push(Length8Digitos);

                    // 16.
                    var ochoDigitID = search.createColumn({
                        name: "formulatext",
                        summary: "GROUP",
                        formula: "{account.custrecord_lmry_co_puc_id}",
                        sort: search.Sort.ASC,
                    });
                    savedsearch.columns.push(ochoDigitID);
                }

            } else {

                // var pucFilter = search.createFilter({
                //   name: 'formulatext',
                //   formula: '{account.custrecord_lmry_co_puc_d4_id}',
                //   operator: search.Operator.STARTSWITH,
                //   values: [paramPUC]
                // });
                // savedsearch.filters.push(pucFilter);

                // //11.
                var exchangerateColum = search.createColumn({
                    name: 'formulacurrency',
                    summary: "GROUP",
                    formula: "{exchangerate}"
                });
                savedsearch.columns.push(exchangerateColum);
                //12.
                var balanceColumn = search.createColumn({
                    name: 'formulacurrency',
                    summary: "SUM",
                    formula: "NVL({debitamount},0) - NVL({creditamount},0)"
                });
                savedsearch.columns.push(balanceColumn);
                // // //13.
                // var multiAccountColumn = search.createColumn({
                //   name: 'account',
                //   join: 'accountingtransaction',
                //   summary: "GROUP"
                // });
                // savedsearch.columns.push(multiAccountColumn);

                if (paramDigits == 2 || paramDigits == 3) {

                    var pucFilter = search.createFilter({
                        name: 'formulatext',
                        formula: '{account.custrecord_lmry_co_puc_d6_id}',
                        operator: search.Operator.STARTSWITH,
                        values: [paramPUC]
                    });
                    savedsearch.filters.push(pucFilter);

                    // 14.
                    var seisDigitID = search.createColumn({
                        name: "formulatext",
                        summary: "GROUP",
                        formula: "{account.custrecord_lmry_co_puc_d6_id}",
                        sort: search.Sort.ASC,
                    });
                    savedsearch.columns.push(seisDigitID);

                    //15.
                    var seisDigitDescripcion = search.createColumn({
                        name: "formulatext",
                        summary: "GROUP",
                        formula: "{account.custrecord_lmry_co_puc_d6_description}",
                        sort: search.Sort.ASC,
                    });
                    savedsearch.columns.push(seisDigitDescripcion);
                }


                if (paramDigits == 3) {
                    // Consultar si obligatoriamente estara con 8 digitos el LATAM CO PUC, para quitar esta validacion que tenga 8 digitos ese latam co puc
                    var Length8Digitos = search.createFilter({
                        name: 'formulatext',
                        formula: 'LENGTH({account.custrecord_lmry_co_puc_id})',
                        operator: search.Operator.IS,
                        values: 8
                    });
                    savedsearch.filters.push(Length8Digitos);

                    // 16.
                    var ochoDigitID = search.createColumn({
                        name: "formulatext",
                        summary: "GROUP",
                        formula: "{account.custrecord_lmry_co_puc_id}",
                        sort: search.Sort.ASC,
                    });
                    savedsearch.columns.push(ochoDigitID);
                }
            }

            var searchresult = savedsearch.run();

            while (!DbolStop) {
                var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

                if (objResult != null && objResult.length != 0) {
                    var intLength = objResult.length;


                    if (intLength != 1000) {
                        DbolStop = true;
                    }

                    for (var i = 0; i < intLength; i++) {
                        var columns = objResult[i].columns;

                        var arrQuiebre = new Array();

                        //PARA 6 DIGITOS
                        if (paramDigits == 2) {
                            for (var col = 0; col < columns.length - 2; col++) {
                                if (featMulti) {
                                    arrQuiebre[0] = objResult[i].getValue(columns[14]);
                                    arrQuiebre[1] = objResult[i].getValue(columns[15]);
                                } else {
                                    arrQuiebre[0] = objResult[i].getValue(columns[13]);
                                    arrQuiebre[1] = objResult[i].getValue(columns[14]);
                                }
                                if (col == 7) {
                                    if (featMulti) {
                                        arrQuiebre[9] = redondear(objResult[i].getValue(columns[12]));
                                    } else {
                                        arrQuiebre[9] = redondear(objResult[i].getValue(columns[7]));
                                    }
                                } else {
                                    arrQuiebre[col + 2] = objResult[i].getValue(columns[col]);
                                }
                            }
                            if (arrQuiebre[9] != 0) {

                                if (featMulti) {
                                    var idAccount = objResult[i].getValue(columns[13]);

                                    if (jsonAccounts[idAccount] != null) {

                                        if (jsonAccounts[idAccount].columna7.substring(0, 1) == paramPUC) {
                                            arrSeisDigitos[_contg] = arrQuiebre;
                                            _contg++;
                                        }

                                    }

                                } else {
                                    arrSeisDigitos[_contg] = arrQuiebre;
                                    _contg++;
                                }
                            }
                        } else if (paramDigits == 3) {
                            // PARA 8 DIGITOS
                            var descripcion8Digit = '';
                            // Obtener la descripcion de 8 digitos  
                            if (featMulti) {
                                descripcion8Digit = jsonDescriptionPuc8[objResult[i].getValue(columns[16])];
                                arrQuiebre[0] = objResult[i].getValue(columns[16]);
                                arrQuiebre[1] = descripcion8Digit;
                                arrQuiebre[2] = objResult[i].getValue(columns[14]);
                                arrQuiebre[3] = objResult[i].getValue(columns[15]);
                            } else {
                                descripcion8Digit = jsonDescriptionPuc8[objResult[i].getValue(columns[15])];
                                arrQuiebre[0] = objResult[i].getValue(columns[15]);
                                arrQuiebre[1] = descripcion8Digit;
                                arrQuiebre[2] = objResult[i].getValue(columns[13]);
                                arrQuiebre[3] = objResult[i].getValue(columns[14]);
                            }

                            for (var col = 0; col < columns.length - 3; col++) {
                                if (col == 7) {
                                    if (featMulti) {
                                        arrQuiebre[11] = redondear(objResult[i].getValue(columns[12]));
                                    } else {
                                        arrQuiebre[11] = redondear(objResult[i].getValue(columns[7]));
                                    }
                                } else {
                                    arrQuiebre[col + 4] = objResult[i].getValue(columns[col]);
                                }
                            }
                            if (arrQuiebre[11] != 0) {

                                if (featMulti) {
                                    var idAccount = objResult[i].getValue(columns[13]);

                                    if (jsonAccounts[idAccount] != null) {

                                        if (jsonAccounts[idAccount].columna9.substring(0, 1) == paramPUC) {
                                            arrOchoDigitos[_contg] = arrQuiebre;
                                            _contg++;
                                        }

                                    }

                                } else {
                                    arrOchoDigitos[_contg] = arrQuiebre;
                                    _contg++;
                                }
                            }

                        } else {
                            //PARA 4 DIGITOS
                            for (var col = 0; col < columns.length; col++) {
                                if (col == 7) {
                                    if (featMulti) {
                                        arrQuiebre[col] = redondear(objResult[i].getValue(columns[12]));
                                    } else {
                                        arrQuiebre[col] = redondear(objResult[i].getValue(columns[7]));
                                    }
                                } else {
                                    arrQuiebre[col] = objResult[i].getValue(columns[col]);
                                }
                            }
                            if (arrQuiebre[7] != 0) {

                                if (featMulti) {
                                    var idAccount = objResult[i].getValue(columns[13]);

                                    if (jsonAccounts[idAccount] != null) {

                                        if (jsonAccounts[idAccount].columna5.substring(0, 1) == paramPUC) {
                                            arrCuatroDigitos[_contg] = arrQuiebre;
                                            _contg++;
                                        }

                                    }

                                } else {
                                    arrCuatroDigitos[_contg] = arrQuiebre;
                                    _contg++;
                                }
                            }
                        }
                    }
                    intDMinReg = intDMaxReg;
                    intDMaxReg += 1000;
                    if (intLength < 1000) {
                        DbolStop = true;
                    }
                } else {
                    DbolStop = true;
                }
            }
            if (paramDigits == 2) {
                return arrSeisDigitos;
            } else if (paramDigits == 3) {
                return arrOchoDigitos;
            } else {
                return arrCuatroDigitos;
            }

        }

        function ObtieneAccountingContext() {
            // Control de Memoria
            var intDMaxReg = 1000;
            var intDMinReg = 0;
            var arrAuxiliar = new Array();
            var contador_auxiliar = 0;

            var DbolStop = false;

            var savedsearch = search.load({
                id: 'customsearch_lmry_account_context'
            });

            // Valida si es OneWorld
            if (featSubsi == true) {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [paramSubsidi]
                });
                savedsearch.filters.push(subsidiaryFilter);
            }

            if (paramDigits == 3) {

                // Para validar que el movimiento obtenido sea mayor que 8 digitos
                var filter8Digitos = search.createFilter({
                    name: 'formulatext',
                    formula: 'LENGTH({custrecord_lmry_co_puc_id})',
                    operator: search.Operator.IS,
                    values: 8
                });
                savedsearch.filters.push(filter8Digitos);


                // Luego del filtro valida que sea de 8 digitos, podemos trae ese campo como ID de 8
                var puc8IdColumn = search.createColumn({
                    name: 'formulatext',
                    formula: '{custrecord_lmry_co_puc_id}',
                    summary: 'GROUP'
                });
                savedsearch.columns.push(puc8IdColumn);
            }


            if (paramDigits == 2 || paramDigits == 3) {
                var puc6IdColumn = search.createColumn({
                    name: 'formulatext',
                    formula: '{custrecord_lmry_co_puc_d6_id}',
                    summary: 'GROUP'
                });
                savedsearch.columns.push(puc6IdColumn);

                var puc6DesColumn = search.createColumn({
                    name: 'formulatext',
                    formula: '{custrecord_lmry_co_puc_d6_description}',
                    summary: 'GROUP'
                });
                savedsearch.columns.push(puc6DesColumn);
            }

            var puc4IdColumn = search.createColumn({
                name: 'formulatext',
                formula: '{custrecord_lmry_co_puc_d4_id}',
                summary: 'GROUP'
            });
            savedsearch.columns.push(puc4IdColumn);

            var puc4DesColumn = search.createColumn({
                name: 'formulatext',
                formula: '{custrecord_lmry_co_puc_d4_description}',
                summary: 'GROUP'
            });
            savedsearch.columns.push(puc4DesColumn);

            var puc2IdColumn = search.createColumn({
                name: 'formulatext',
                formula: '{custrecord_lmry_co_puc_d2_id}',
                summary: 'GROUP'
            });
            savedsearch.columns.push(puc2IdColumn);

            var puc2DesColumn = search.createColumn({
                name: 'formulatext',
                formula: '{custrecord_lmry_co_puc_d2_description}',
                summary: 'GROUP'
            });
            savedsearch.columns.push(puc2DesColumn);

            var puc1IdColumn = search.createColumn({
                name: 'formulatext',
                formula: '{custrecord_lmry_co_puc_d1_id}',
                summary: 'GROUP'
            });
            savedsearch.columns.push(puc1IdColumn);

            var puc1DesColumn = search.createColumn({
                name: 'formulatext',
                formula: '{custrecord_lmry_co_puc_d1_description}',
                summary: 'GROUP'
            });
            savedsearch.columns.push(puc1DesColumn);

            var searchresult = savedsearch.run();

            while (!DbolStop) {
                var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

                if (objResult != null) {
                    var intLength = objResult.length;
                    if (intLength == 0) {
                        DbolStop = true;
                    } else {
                        if (paramDigits == 2) {
                            for (var i = 0; i < intLength; i++) {
                                // Cantidad de columnas de la busqueda
                                var columns = objResult[i].columns;
                                arrAuxiliar = new Array();
                                for (var col = 0; col < columns.length; col++) {
                                    if (col == 3) {
                                        arrAuxiliar[col] = objResult[i].getText(columns[col]);
                                    } else {
                                        arrAuxiliar[col] = objResult[i].getValue(columns[col]);
                                    }
                                }

                                if (arrAuxiliar[3] == multibookName) {
                                    arrAccountingContext[contador_auxiliar] = arrAuxiliar;
                                    contador_auxiliar++;
                                }
                            }
                        } else if (paramDigits == 3) {
                            for (var i = 0; i < intLength; i++) {
                                // Cantidad de columnas de la busqueda
                                var columns = objResult[i].columns;
                                var description8digit = jsonDescriptionPuc8[objResult[i].getValue(columns[4])];
                                arrAuxiliar = new Array();
                                for (var col = 0; col < columns.length + 1; col++) {
                                    if (col == 3) {
                                        arrAuxiliar[col] = objResult[i].getText(columns[col]);
                                    } else if (col == 5) {
                                        arrAuxiliar[col] = description8digit;
                                    } else if (col > 5) {
                                        arrAuxiliar[col] = objResult[i].getValue(columns[col - 1]);
                                    } else {
                                        arrAuxiliar[col] = objResult[i].getValue(columns[col]);
                                    }
                                }

                                if (arrAuxiliar[3] == multibookName) {
                                    arrAccountingContext[contador_auxiliar] = arrAuxiliar;
                                    contador_auxiliar++;
                                }
                            }
                        } else {
                            for (var i = 0; i < intLength; i++) {
                                // Cantidad de columnas de la busqueda
                                var columns = objResult[i].columns;
                                arrAuxiliar = new Array();
                                for (var col = 0; col < columns.length; col++) {
                                    if (col == 3) {
                                        arrAuxiliar[col] = objResult[i].getText(columns[col]);
                                    } else {
                                        arrAuxiliar[col] = objResult[i].getValue(columns[col]);
                                    }
                                }

                                if (arrAuxiliar[3] == multibookName) {
                                    arrAccountingContext[contador_auxiliar] = arrAuxiliar;
                                    contador_auxiliar++;
                                }
                            }
                        }
                    }
                    intDMinReg = intDMaxReg;
                    intDMaxReg += 1000;

                } else {
                    DbolStop = true;
                }
            }
        }

        function obtenerCuenta(numero_cuenta) {

            for (var i = 0; i < arrAccountingContext.length; i++) {
                if (numero_cuenta == arrAccountingContext[i][0]) {
                    var number_cta_aux = arrAccountingContext[i][1];
                    for (var j = 0; j < arrAccountingContext.length; j++) {

                        if (number_cta_aux == arrAccountingContext[j][0]) {
                            var ArrPuc = new Array();
                            if (paramDigits == 2) {
                                ArrPuc[0] = arrAccountingContext[j][4];
                                ArrPuc[1] = arrAccountingContext[j][5];
                                ArrPuc[2] = arrAccountingContext[j][6];
                                ArrPuc[3] = arrAccountingContext[j][7];
                                ArrPuc[4] = arrAccountingContext[j][8];
                                ArrPuc[5] = arrAccountingContext[j][9];
                                ArrPuc[6] = arrAccountingContext[j][10];
                                ArrPuc[7] = arrAccountingContext[j][11];
                            } else if (paramDigits == 3) {
                                ArrPuc[0] = arrAccountingContext[j][4];
                                ArrPuc[1] = arrAccountingContext[j][5];
                                ArrPuc[2] = arrAccountingContext[j][6];
                                ArrPuc[3] = arrAccountingContext[j][7];
                                ArrPuc[4] = arrAccountingContext[j][8];
                                ArrPuc[5] = arrAccountingContext[j][9];
                                ArrPuc[6] = arrAccountingContext[j][10];
                                ArrPuc[7] = arrAccountingContext[j][11];
                                ArrPuc[8] = arrAccountingContext[j][12];
                                ArrPuc[9] = arrAccountingContext[j][13];
                            } else {
                                ArrPuc[0] = arrAccountingContext[j][4];
                                ArrPuc[1] = arrAccountingContext[j][5];
                                ArrPuc[2] = arrAccountingContext[j][6];
                                ArrPuc[3] = arrAccountingContext[j][7];
                                ArrPuc[4] = arrAccountingContext[j][8];
                                ArrPuc[5] = arrAccountingContext[j][9];
                            }
                            return ArrPuc;
                        }
                    }
                }
            }
            return numero_cuenta;
        }

        function CambioDeCuentas(arrDigitos) {
            for (var i = 0; i < arrDigitos.length; i++) {

                if (paramDigits == 2) {
                    // Para 6 digitos
                    // arrDigitos[i][10] ES EL TIPO DE CUENTA (ACCOUNT TYPE) DE CADA TRANSACTION
                    // arrDigitos[i][11] ES EL NUMERO DE CUENTA (ACCOUNT NUMBER) DE CADA TRANSACTION
                    if (arrDigitos[i][10] == 'Bank' || arrDigitos[i][10] == 'Accounts Payable' || arrDigitos[i][10] == 'Accounts Receivable' ||
                        arrDigitos[i][10] == 'Banco' || arrDigitos[i][10] == 'Cuentas a pagar' || arrDigitos[i][10] == 'Cuentas a cobrar') {
                        var cuenta_act = obtenerCuenta(arrDigitos[i][11]);
                        if (cuenta_act != arrDigitos[i][11]) {
                            arrDigitos[i][0] = cuenta_act[0];
                            arrDigitos[i][1] = cuenta_act[1];
                            arrDigitos[i][2] = cuenta_act[2];
                            arrDigitos[i][3] = cuenta_act[3];
                            arrDigitos[i][4] = cuenta_act[4];
                            arrDigitos[i][5] = cuenta_act[5];
                            arrDigitos[i][6] = cuenta_act[6];
                            arrDigitos[i][7] = cuenta_act[7];
                        }
                    }
                } else if (paramDigits == 3) {
                    // REVISAR ESTO, PARA  CAMBIAR DEPENDIENDO LO QUE TRAIGA
                    // Para 8 digitos
                    // arrDigitos[i][12] ES EL TIPO DE CUENTA (ACCOUNT TYPE) DE CADA TRANSACTION
                    // arrDigitos[i][13] ES EL NUMERO DE CUENTA (ACCOUNT NUMBER) DE CADA TRANSACTION                  

                    if (arrDigitos[i][12] == 'Bank' || arrDigitos[i][12] == 'Accounts Payable' || arrDigitos[i][12] == 'Accounts Receivable' ||
                        arrDigitos[i][12] == 'Banco' || arrDigitos[i][12] == 'Cuentas a pagar' || arrDigitos[i][12] == 'Cuentas a cobrar') {
                        var cuenta_act = obtenerCuenta(arrDigitos[i][13]);
                        if (cuenta_act != arrDigitos[i][13]) {
                            arrDigitos[i][0] = cuenta_act[0];
                            arrDigitos[i][1] = cuenta_act[1];
                            arrDigitos[i][2] = cuenta_act[2];
                            arrDigitos[i][3] = cuenta_act[3];
                            arrDigitos[i][4] = cuenta_act[4];
                            arrDigitos[i][5] = cuenta_act[5];
                            arrDigitos[i][6] = cuenta_act[6];
                            arrDigitos[i][7] = cuenta_act[7];
                            arrDigitos[i][8] = cuenta_act[8];
                            arrDigitos[i][9] = cuenta_act[9];
                        }
                    }

                } else {
                    // Para 4 digitos
                    // arrDigitos[i][8] ES EL TIPO DE CUENTA (ACCOUNT TYPE) DE CADA TRANSACTION
                    // arrDigitos[i][9] ES EL NUMERO DE CUENTA (ACCOUNT NUMBER) DE CADA TRANSACTION
                    if (arrDigitos[i][8] == 'Bank' || arrDigitos[i][8] == 'Accounts Payable' || arrDigitos[i][8] == 'Accounts Receivable' ||
                        arrDigitos[i][8] == 'Banco' || arrDigitos[i][8] == 'Cuentas a pagar' || arrDigitos[i][8] == 'Cuentas a cobrar') {
                        var cuenta_act = obtenerCuenta(arrDigitos[i][9]);
                        if (cuenta_act != arrDigitos[i][9]) {
                            arrDigitos[i][0] = cuenta_act[0];
                            arrDigitos[i][1] = cuenta_act[1];
                            arrDigitos[i][2] = cuenta_act[2];
                            arrDigitos[i][3] = cuenta_act[3];
                            arrDigitos[i][4] = cuenta_act[4];
                            arrDigitos[i][5] = cuenta_act[5];
                        }
                    }
                }
            }
        }

        function OrdenarCuentas(arrDigitos) {
            arrDigitos.sort(sortFunction);

            function sortFunction(a, b) {
                if (a[0] === b[0]) {
                    return 0;
                } else {
                    return (a[0] < b[0]) ? -1 : 1;
                }
            }
            return arrDigitos;
        }


        function Name_File() {
            var _NameFile = '';

            var fecha_format = format.parse({
                value: periodenddate,
                type: format.Type.DATE
            });

            var MM = fecha_format.getMonth() + 1;
            var YYYY = fecha_format.getFullYear();
            // var DD = fecha_format.getDate();
            if (('' + MM).length == 1) {
                MM = '0' + MM;
            }
            //var  MesConvertido = Periodo(MM);
            _NameFile = "COLibroInventarioBalance" + '_' + 1 + '_' + companyname + '_' + MM + '_' + YYYY;
            return _NameFile;
        }

        function saveFile(data, nameFile, extension) {
            // Ruta de la carpeta contenedora
            var FolderId = objContext.getParameter({
                name: 'custscript_lmry_file_cabinet_rg_co'
            });

            // log.debug('FolderId',FolderId);
            // Almacena en la carpeta de Archivos Generados
            if (FolderId != '' && FolderId != null) {
                // Crea el archivo.xls
                if (extension == 'txt') {
                    var file = fileModulo.create({
                        name: nameFile + '.' + extension,
                        fileType: fileModulo.Type.PLAINTEXT,
                        contents: data,
                        folder: FolderId
                    });
                } else {
                    var file = fileModulo.create({
                        name: nameFile + '.' + extension,
                        fileType: fileModulo.Type.EXCEL,
                        contents: data,
                        folder: FolderId
                    });
                }
                // Termina de grabar el archivo
                var idfile = file.save();
                // log.debug('Se actualizo archivo temporal con id: ', idfile);
                // Trae URL de archivo generado
                if (extension == 'xls') {
                    var idfile2 = fileModulo.load({
                        id: idfile
                    });
                    // Obtenemos de las prefencias generales el URL de Netsuite (Produccion o Sandbox)
                    var getURL = objContext.getParameter({
                        name: 'custscript_lmry_netsuite_location'
                    });

                    var urlfile = '';
                    if (getURL != '' && getURL != '') {
                        urlfile += 'https://' + getURL;
                    }
                    urlfile += idfile2.url;

                    //Genera registro personalizado como log
                    if (idfile) {
                        var usuarioTemp = runtime.getCurrentUser();
                        var id = usuarioTemp.id;
                        var employeename = search.lookupFields({
                            type: search.Type.EMPLOYEE,
                            id: id,
                            columns: ['firstname', 'lastname']
                        });
                        var usuario = employeename.firstname + ' ' + employeename.lastname;

                        var record = recordModulo.load({
                            type: 'customrecord_lmry_co_rpt_generator_log',
                            id: paramLogId
                        });
                        //Nombre de Archivo
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_rg_name',
                            value: nameFile + '.' + extension
                        });
                        //Nombre de Reporte
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_rg_transaction',
                            value: 'CO - Libro de Inventario y Balance 2.0'
                        });
                        //Periodo
                        // record.setValue({
                        //     fieldId: 'custrecord_lmry_co_rg_postingperiod',
                        //     value: Fecha_Corte_al
                        // });
                        //Nombre de Subsidiaria
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_rg_subsidiary',
                            value: companyname
                        });
                        //Url de Archivo
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_rg_url_file',
                            value: urlfile
                        });
                        //Multibook
                        if (featMulti || featMulti == 'T') {
                            record.setValue({
                                fieldId: 'custrecord_lmry_co_rg_multibook',
                                value: multibookName
                            });
                        }
                        //Creado Por
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_rg_employee',
                            value: usuario
                        });

                        var recordId = record.save();
                        // Envia mail de conformidad al usuario
                        libFeature.sendConfirmUserEmail(nameFile + '.' + extension, 3, 'CO - Libro de Inventario y Balance 2.0', language)
                    }
                } else if (extension == "noExisteData") {
                    //Cuando no hay datos para los criterios ingresados
                    var record = recordModulo.load({
                        type: 'customrecord_lmry_co_rpt_generator_log',
                        id: paramLogId
                    });
                    //Nombre de Archivo
                    record.setValue({
                        fieldId: 'custrecord_lmry_co_rg_name',
                        value: nameFile
                    });
                    var recordId = record.save();
                }

            } else {
                // Debug
                log.debug({
                    title: 'DEBUG',
                    details: 'Creacion de Txt' +
                        'No se existe el folder'
                });
            }
        }

        function updateReportLog(msg) {

            var usuarioTemp = runtime.getCurrentUser();

            var id = usuarioTemp.id;
            var employeename = search.lookupFields({
                type: search.Type.EMPLOYEE,
                id: id,
                columns: ['firstname', 'lastname']
            });
            var usuario = employeename.firstname + ' ' + employeename.lastname;

            var record = recordModulo.load({
                type: 'customrecord_lmry_co_rpt_generator_log',
                id: paramLogId
            });

            //Nombre de Archivo
            record.setValue({
                fieldId: 'custrecord_lmry_co_rg_name',
                value: msg
            });
            //Nombre de Reporte
            record.setValue({
                fieldId: 'custrecord_lmry_co_rg_transaction',
                value: 'CO - Libro de Inventario y Balance 2.0'
            });
            //Creado Por
            record.setValue({
                fieldId: 'custrecord_lmry_co_rg_employee',
                value: usuario
            });

            var recordId = record.save();
        }

        function ObtenerDatosSubsidiaria() {

            var configpage = config.load({
                type: config.Type.COMPANY_INFORMATION
            });
            if (featSubsi) {
                companyname = ObtainNameSubsidiaria(paramSubsidi);
                companyruc = ObtainFederalIdSubsidiaria(paramSubsidi);

            } else {
                companyruc = configpage.getValue('employerid');
                companyname = configpage.getValue('legalname');

            }
            companyruc = companyruc.replace(' ', '');
            companyname = companyname.replace(' ', '');

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

        function ObtenerParametrosYFeatures() {
            //Parametros
            paramSubsidi = objContext.getParameter({
                name: 'custscript_lmry_co_invbalv2_subsi'
            });
            paramPeriodo = objContext.getParameter({
                name: 'custscript_lmry_co_invbalv2_periodo'
            });
            paramPeriodsRestantes = objContext.getParameter({
                name: 'custscript_lmry_co_invbalv2_period_res'
            });
            paramMulti = objContext.getParameter({
                name: 'custscript_lmry_co_invbalv2_multibook'
            });
            paramLogId = objContext.getParameter({
                name: 'custscript_lmry_co_invbalv2_logid'
            });
            paramPUC = objContext.getParameter({
                name: 'custscript_lmry_co_invbalv2_puc' //primer digito
            });
            paramFile = objContext.getParameter({
                name: 'custscript_lmry_co_invbalv2_fileid'
            });
            paramAdjustment = objContext.getParameter({
                name: 'custscript_lmry_co_invbalv2_adjust'
            });
            paramDigits = objContext.getParameter({
                name: 'custscript_lmry_co_invbalv2_digits'
            });

            //Features
            featSubsi = runtime.isFeatureInEffect({
                feature: "SUBSIDIARIES"
            });
            featMulti = runtime.isFeatureInEffect({
                feature: "MULTIBOOK"
            });
            featurePeriodEnd = runtime.isFeatureInEffect({
                feature: "PERIODENDJOURNALENTRIES"
            });
            featureCalendars = runtime.isFeatureInEffect({
                feature: "MULTIPLECALENDARS"
            });

            // log.debug('Scripts params:', paramAdjustment + '-'+ paramLogId + '-' + paramMulti + '-' + paramSubsidi + '-' + paramPeriodo + '-' + paramPeriodsRestantes + '-' + paramPUC + '-' + paramFile);

            jsonParametros = {
                paramSubsidi: paramSubsidi,
                paramPeriodo: paramPeriodo,
                paramPeriodsRestantes: paramPeriodsRestantes,
                paramMulti: paramMulti,
                paramLogId: paramLogId,
                paramPUC: paramPUC,
                paramFile: paramFile,
                paramAdjustment: paramAdjustment,
                paramAdjustment: paramAdjustment,
                paramDigits: paramDigits
            };

            log.debug('COMIENZA BUCLE:', jsonParametros);

            var licenses = libFeature.getLicenses(paramSubsidi);
            featAccountingSpecial = libFeature.getAuthorization(677, licenses);

            // Si esta marcado checkbox de 8 digitos y no tiene ningun CO PUC de 8 digitos
            var configuracion8digit = configuracion8PUCs();
            log.debug('configuracion8digit', configuracion8digit);

            if (paramDigits == 3) {
                if (!configuracion8digit) {
                    saveFile("", "No existe informacion para los criterios seleccionados", "noExisteData");
                    error8digitos = true;
                }
            }

            if (featureCalendars || featureCalendars == 'T') {
                if (featSubsi || featSubsi == 'T') {
                    var subsidiary = search.lookupFields({
                        type: search.Type.SUBSIDIARY,
                        id: paramSubsidi,
                        columns: ['fiscalcalendar']
                    });

                    calendarSubsi = subsidiary.fiscalcalendar[0].value;
                }
            }

            if (featAccountingSpecial || featAccountingSpecial == 'T') {
                var searchSpecialPeriod = search.create({
                    type: "customrecord_lmry_special_accountperiod",
                    filters: [
                        ["isinactive", "is", "F"], 'AND', ["custrecord_lmry_accounting_period", "is", paramPeriodo]
                    ],
                    columns: [
                        search.createColumn({
                            name: "custrecord_lmry_calendar",
                            label: "0. Latam - Calendar"
                        }),
                        search.createColumn({
                            name: "custrecord_lmry_date_ini",
                            label: "1. Latam - Date Start",
                        }),
                        search.createColumn({
                            name: "custrecord_lmry_date_fin",
                            label: "2. Latam - Date Fin",
                        }),
                        search.createColumn({
                            name: "name",
                            label: "3. Latam - Period Name",
                        })
                    ]
                });

                var pagedData = searchSpecialPeriod.runPaged({
                    pageSize: 1000
                });

                pagedData.pageRanges.forEach(function (pageRange) {
                    page = pagedData.fetch({
                        index: pageRange.index
                    });

                    page.data.forEach(function (result) {
                        columns = result.columns;
                        var calendar = result.getValue(columns[0]);

                        if (calendar != null && calendar != '') {
                            calendar = JSON.parse(calendar);
                            if (calendar.id == calendarSubsi) {
                                periodname = result.getValue(columns[3]);
                                periodenddate = result.getValue(columns[2]);
                                periodstartdate = result.getValue(columns[1]);
                            }
                        } else {
                            periodname = result.getValue(columns[3]);
                            periodenddate = result.getValue(columns[2]);
                            periodstartdate = result.getValue(columns[1]);
                        }

                    })
                });

            } else {
                //Period enddate para el nombre del libro
                var period_temp = search.lookupFields({
                    type: search.Type.ACCOUNTING_PERIOD,
                    id: paramPeriodo,
                    columns: ['periodname', 'startdate', 'enddate']
                });

                periodenddate = period_temp.enddate;
                periodstartdate = period_temp.startdate;
                periodname = period_temp.periodname;

            }

            language = runtime.getCurrentScript().getParameter("LANGUAGE").substring(0, 2);

            var fecha_format = format.parse({
                value: periodstartdate,
                type: format.Type.DATE
            });

            var MM = fecha_format.getMonth() + 1;
            var YYYY = fecha_format.getFullYear();

            if (('' + MM).length == 1) {
                MM = '0' + MM;
            }

            Fecha_Corte_al = periodname;

            if (paramDigits == '3') {
                obtenerDescript8Digitos();
                log.debug('jsonDescriptionPuc8', jsonDescriptionPuc8);
            }

            //Multibook Name
            if (featMulti) {
                var multibookName_temp = search.lookupFields({
                    type: search.Type.ACCOUNTING_BOOK,
                    id: paramMulti,
                    columns: ['name']
                });

                multibookName = multibookName_temp.name;
            }

            var result_f_temp = search.create({
                type: search.Type.CURRENCY,
                columns: ['name', 'symbol']
            });
            var result_f_temp2 = result_f_temp.run();
            result_f = result_f_temp2.getRange(0, 1000);

        }

        function obtenerDescript8Digitos() {

            var DbolStop = false;
            var intDMinReg = 0;
            var intDMaxReg = 1000;

            // Sacar descripcion de digito 8
            var descripcion8digitos = search.create({
                type: "customrecord_lmry_co_puc",
                filters: [],
                columns: [
                    search.createColumn({ name: "name", label: "Name" }),
                    search.createColumn({ name: "custrecord_lmry_co_puc", label: "CO PUC Description" })
                ]
            });

            var filterIsNotEmptyPUCd8 = search.createFilter({
                name: "formulatext",
                formula: "LENGTH({name})",
                operator: search.Operator.IS,
                values: 8
            });
            descripcion8digitos.filters.push(filterIsNotEmptyPUCd8);

            var savedsearch = descripcion8digitos.run();

            while (!DbolStop) {
                var objResult = savedsearch.getRange(intDMinReg, intDMaxReg);
                if (objResult != null) {
                    if (objResult.length != 1000) {
                        DbolStop = true;
                    }

                    for (var i = 0; i < objResult.length; i++) {
                        var columns = objResult[i].columns;
                        var arrAuxiliar = new Array();
                        var name = '';
                        var description = '';
                        //0. NAME
                        if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '') {
                            name = objResult[i].getValue(columns[0]);
                        }

                        //1. DESCRIPCION
                        if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '') {
                            description = objResult[i].getValue(columns[1]);
                        }

                        jsonDescriptionPuc8[name] = description;
                    }

                    if (!DbolStop) {
                        intDMinReg = intDMaxReg;
                        intDMaxReg += 1000;
                    }
                } else {
                    DbolStop = true;
                }
            }
        }


        function configuracion8PUCs() {
            var configuracionPuc = search.create({
                type: "customrecord_lmry_co_puc",
                filters: [
                    ["isinactive", "is", "F"]
                ],
                columns: [
                    search.createColumn({
                        name: "name",
                        sort: search.Sort.ASC,
                        label: "0. PUC 8D"
                    })
                ]
            });
            var Length8Digitos = search.createFilter({
                name: 'formulatext',
                formula: 'LENGTH({name})',
                operator: search.Operator.IS,
                values: 8
            });
            configuracionPuc.filters.push(Length8Digitos);

            var savedSearch = configuracionPuc.run();
            var objResult = savedSearch.getRange(0, 1000);
            var cantidadSearch = objResult.length;
            if (cantidadSearch > 0) {
                return true;
            } else {
                return false;
            }
        }

        function dividirArray(arrayData, divisor) {

            var arrayResult = new Array();
            var tam = arrayData.length;
            var ini = 0;
            var fin = divisor

            while (fin <= tam) {
                var partArray = arrayData.slice(ini, fin);
                arrayResult.push(partArray);

                ini = fin;
                fin += divisor;
            }

            if (ini < tam) {
                var partArray = arrayData.slice(ini, fin);
                arrayResult.push(partArray);
            }

            return arrayResult;
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
            execute: execute
        };
    });