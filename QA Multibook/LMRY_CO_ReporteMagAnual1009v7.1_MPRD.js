/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_ReporteMagAnual1009v7.1_MPRD.js          ||
||                                                              ||
||  Version Date           Author        Remarks                ||
||  2.0     Marzo 29 2019  LatamReady    Use Script 2.0         ||
 \= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */

/**
 *@NApiVersion 2.0
 *@NScriptType MapReduceScript
 *@NModuleScope Public
 */

define(['N/record', 'N/runtime', 'N/file', 'N/email', 'N/search', 'N/encode', 'N/currency',
        'N/format', 'N/log', 'N/config', 'N/xml', 'N/task', './CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js'
    ],

    function(record, runtime, file, email, search, encode, currency, format, log, config, xml, task, libreria) {

        var LMRY_script = "LMRY_CO_ReporteMagAnual1009v7.1_MPRD.js";

        var objContext = runtime.getCurrentScript();
        // paramSubsidiaria + ' - ' + paramMultibook + ' - ' + paramPeriodo + ' - ' + paramIdReport + ' - ' + paramBucle + ' - ' + paramCont + ' - ' + paramIdFeatureByVersion + ' - ' + paramConcepto);
        // 12 - 3 - 2020 - 39 - 0 - 0 - 12 - 1
        var paramSubsidiaria = objContext.getParameter('custscript_lmry_1009_v71_subsi');
        var paramPeriodo = objContext.getParameter('custscript_lmry_1009_v71_period');
        var paramMultibook = objContext.getParameter('custscript_lmry_1009_v71_multi');
        var paramIdReport = objContext.getParameter('custscript_lmry_1009_v71_rpt');
        var paramIdLog = objContext.getParameter('custscript_lmry_1009_v71_idlog');
        var paramConcepto = objContext.getParameter('custscript_lmry_1009_v71_concept');
        var paramIdFeatureByVersion = objContext.getParameter('custscript_lmry_1009_v71_byversion');

        var isSubsidiariaFeature = runtime.isFeatureInEffect({
            feature: 'SUBSIDIARIES'
        });
        var isMultibookFeature = runtime.isFeatureInEffect({
            feature: 'MULTIBOOK'
        });
        var hasJobsFeature = runtime.isFeatureInEffect({
            feature: 'JOBS'
        });
        var hasAdvancedJobsFeature = runtime.isFeatureInEffect({
            feature: 'ADVANCEDJOBS'
        });

        var language = runtime.getCurrentScript().getParameter("LANGUAGE").substring(0, 2);
        var GLOBAL_LABELS = {};

        function getInputData() {
            try {
                log.debug('Entro al ', 'getInputDAta');
                //Obtener cuentas
                var accountArray = ObtenerCuentas();
                log.debug('accountArray ', accountArray);
                var arr_MediosMagneticos = ObtenerCuentasXPagarPC(accountArray[0]) || [];
                log.debug('arr_MediosMagneticos ', arr_MediosMagneticos.length);
                var arrCuentas_x_Pagar_Bancos = ObtenerConceptos20032004(accountArray[1]) || [];
                log.debug('arrCuentas_x_Pagar_Bancos ', arrCuentas_x_Pagar_Bancos.length);

                var transactionArray = arr_MediosMagneticos.concat(arrCuentas_x_Pagar_Bancos);
                log.debug('transactionArray', transactionArray.length);

                if (transactionArray.length != 0) {
                    return transactionArray;
                } else {
                    NoData();
                }
            } catch (error) {
                log.error('FIX ME', error);
                return [{
                    isError: "T",
                    error: error
                }];
            }

        }

        function map(context) {

            try {

                //log.error('Entro al ', 'map');
                var Key_ = JSON.parse(context.key);
                var objResult = JSON.parse(context.value);

                if (objResult["isError"] == "T") {
                    context.write({
                        key: context.key,
                        value: objResult
                    });
                } else {

                    if (objResult[objResult.length - 1] == 'MediosMagneticos') {

                        //log.debug('MMtransaction', objResult);
                        var accountDetailarr = getInformationCuentasxPagar(objResult);
                        //log.debug('MM_DetailJson', accountDetailarr);

                        context.write({
                            key: context.key,
                            value: accountDetailarr
                        });


                    } else if (objResult[objResult.length - 1] == 'Bank_concept') {

                        context.write({
                            key: context.key,
                            value: objResult.slice(0, 14)
                        });


                    }

                }

            } catch (error) {
                log.error("error map", error);
                log.error("error objResult map", objResult);
                log.error("error key", context.key);
                context.write({
                    key: context.key,
                    value: {
                        isError: "T",
                        error: error
                    }
                });
            }

        }

        function reduce(context) {
            //'1||||||||':[,,,,,,va1,va2,va3,va4],[,,,,,,va3,va2y,vay3,vay4],..]

            try {

            } catch (error) {
                context.write({
                    key: context.key,
                    value: {
                        isError: "T",
                        error: error
                    }
                });
            }

        }

        function summarize(context) {

            log.debug('Entro al ', 'summarize');

            try {

                var arr_final = [];
                var cuantiasJson = {};
                var arr_aux_cuantias = [];
                var valorTotal = 0;
                var errores = [];

                if (paramIdFeatureByVersion) {
                    var filterByVersion = search.lookupFields({
                        type: 'customrecord_lmry_co_rpt_feature_version',
                        id: paramIdFeatureByVersion,
                        columns: ['custrecord_lmry_co_amount']
                    });
                    var CUANTIA_MINIMA = filterByVersion.custrecord_lmry_co_amount;
                }
                log.debug('CO LEAST AMOUNT', CUANTIA_MINIMA);

                context.output.iterator().each(function(key, value) {
                    var objResult = JSON.parse(value);
                    if (objResult["isError"] == "T") {
                        errores.push(JSON.stringify(objResult["error"]));
                    } else {
                        var concepto = objResult[0];

                        if (Math.abs(objResult[13]) <= CUANTIA_MINIMA) {
                            if (cuantiasJson[concepto] === undefined) {
                                cuantiasJson[concepto] = objResult[13];
                            } else {
                                cuantiasJson[concepto] = round(cuantiasJson[concepto] + objResult[13]);
                            }

                        } else {
                            var arr_aux = objResult;
                            valorTotal += arr_aux[13];
                            arr_aux[13] = Math.abs(arr_aux[13]).toFixed(0);
                            arr_final.push(arr_aux);
                        }
                    }
                    return true;
                });

                log.error("Numero de errores", errores.length);

                for (key in cuantiasJson) {
                    if (Math.abs(cuantiasJson[key]) != 0) {
                        valorTotal += cuantiasJson[key];
                        arr_aux_cuantias = GenerarCuentasxPagarPorCuantias(key, cuantiasJson[key]);
                        arr_final.push(arr_aux_cuantias);
                    }
                }

                //log.error("valorTotal", valorTotal);

                if (arr_final.length != 0) {
                    log.debug("valorTotal", valorTotal);
                    var numeroEnvio = ObtenerNumeroEnvio();
                    GLOBAL_LABELS = getGlobalLabels();
                    GenerarExcel(arr_final, numeroEnvio);
                    GenerarXml(arr_final, numeroEnvio, valorTotal);
                } else {
                    NoData();
                }

                log.debug('Termino el ', 'summarize');

            } catch (error) {
                log.error("error", error);
            }
        }

        function getGlobalLabels() {
            var labels = {
                "titulo": {
                    "es": 'FORMULARIO 1009: SALDO DE CUENTAS POR PAGAR',
                    "pt": 'FORMULARIO 1009: SALDO DE CONTAS A PAGAR',
                    "en": 'FORM 1009: BALANCE OF ACCOUNTS PAYABLE'
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
                "concepto": {
                    "es": 'Concepto',
                    "pt": 'Conceito',
                    "en": 'Concept'
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
                "direccion": {
                    "es": 'Direccion',
                    "pt": 'Endereco',
                    "en": 'Address'
                },
                "pais": {
                    "es": 'Pais',
                    "pt": 'Pais',
                    "en": 'Country'
                },
                "departamento": {
                    "es": 'Depto',
                    "pt": 'Departamento',
                    "en": 'Department'
                },
                "saldos": {
                    "es": 'Saldos',
                    "pt": 'Saldos',
                    "en": 'Balances'
                },
                'noData': {
                    "es": 'No existe informacion para los criterios seleccionados',
                    "pt": 'Não há informações para os critérios selecionados',
                    "en": 'There is no information for the selected criteria'
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
            }

            return labels;
        }

        function GenerarCuentasxPagarPorCuantias(key, objResult) {
            var cuantiaArray = [];
            //log.debug("parametros de subsi", paramSubsidiaria);
            var auxAddress = obtenerSubsidiariaAddress();
            cuantiaArray = [];
            cuantiaArray[0] = key;
            cuantiaArray[1] = '43';
            cuantiaArray[2] = '222222222';
            cuantiaArray[3] = '';
            cuantiaArray[4] = '';
            cuantiaArray[5] = '';
            cuantiaArray[6] = '';
            cuantiaArray[7] = '';
            cuantiaArray[8] = 'CUANTIAS MENORES';
            cuantiaArray[9] = auxAddress[0];
            cuantiaArray[10] = auxAddress[1];
            cuantiaArray[11] = auxAddress[2];
            cuantiaArray[12] = auxAddress[3];
            cuantiaArray[13] = Math.abs(objResult).toFixed(0);

            return cuantiaArray;
        }

        function ObtenerCuentasXPagarPC(jsonArr) {

            var accountsArray = Object.keys(jsonArr);
            log.debug("accountsArray ctasxPagar", accountsArray);

            /*LatamReady - CO Form 1009 Balance of Accounts Payable V7.1 V2 */

            var savedSearch = search.load({
                id: 'customsearch_lmry_co_form_1009_pay_v71'
            });

            if (paramPeriodo) {

                var periodEndDate = new Date(paramPeriodo, 11, 31);

                periodEndDate = format.format({
                    value: periodEndDate,
                    type: format.Type.DATE
                });

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

            var vendorColumn = search.createColumn({
                name: 'formulanumeric',
                formula: "CASE WHEN CONCAT ({Type.id},'') = 'ExpRept' THEN {custcol_lmry_exp_rep_vendor_colum.internalid} ELSE CASE WHEN CONCAT ({Type.id},'') = 'Check' THEN {mainname.id} ELSE NVL({vendor.internalid},{vendorline.internalid}) END END",
                summary: 'GROUP'
            });
            savedSearch.columns.push(vendorColumn);

            if (hasJobsFeature && !hasAdvancedJobsFeature) {
                log.debug("customermain");
                var customerColumn = search.createColumn({
                    name: 'formulanumeric',
                    formula: 'NVL({customermain.internalid},{customer.internalid})',
                    summary: 'GROUP'
                });
                savedSearch.columns.push(customerColumn);
            } else if ((!hasJobsFeature && !hasAdvancedJobsFeature) || (!hasJobsFeature && hasAdvancedJobsFeature) || (hasJobsFeature && hasAdvancedJobsFeature)) {
                log.debug("customer JOB");
                var customerColumn = search.createColumn({
                    name: "formulanumeric",
                    formula: "CASE WHEN NVL({job.internalid},-1) = -1 THEN {customer.internalid} ELSE {job.customer.id} end",
                    summary: "GROUP"
                });
                savedSearch.columns.push(customerColumn);
            }
            var EmployeeColumn = search.createColumn({
                name: 'formulanumeric',
                formula: "{entity.id}",
                summary: 'GROUP'
            });
            savedSearch.columns.push(EmployeeColumn);

            if (isMultibookFeature) {

                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [paramMultibook]
                });
                savedSearch.filters.push(multibookFilter);

                if (accountsArray.length != 0) {

                    var accountFilter = search.createFilter({
                        name: 'account',
                        join: 'accountingtransaction',
                        operator: search.Operator.ANYOF,
                        values: accountsArray
                    });
                    savedSearch.filters.push(accountFilter);

                } else {
                    log.debug("accountsArray es 0", "accountsArray es 0");
                    var accountFilter = search.createFilter({
                        name: 'account',
                        join: 'accountingtransaction',
                        operator: search.Operator.ANYOF,
                        values: ['-1']
                    });
                    savedSearch.filters.push(accountFilter);
                }

                var accountIdColumn = search.createColumn({
                    name: 'formulanumeric',
                    summary: 'GROUP',
                    formula: '{accountingtransaction.account.id}',
                });
                savedSearch.columns.splice(1, 1, accountIdColumn);

                var column2 = search.createColumn({
                    name: 'formulacurrency',
                    summary: 'SUM',
                    formula: "NVL({accountingtransaction.debitamount},0)-NVL({accountingtransaction.creditamount},0)"
                });
                savedSearch.columns.splice(2, 1, column2);

            } else {

                if (accountsArray.length != 0) {
                    var accountFilter = search.createFilter({
                        name: 'account',
                        operator: search.Operator.ANYOF,
                        values: accountsArray
                    });
                    savedSearch.filters.push(accountFilter);

                } else {
                    log.debug("accountsArray es 0", "accountsArray es 0");
                    var accountFilter = search.createFilter({
                        name: 'account',
                        operator: search.Operator.ANYOF,
                        values: ['-1']
                    });
                    savedSearch.filters.push(accountFilter);
                }

            }

            var pagedData = savedSearch.runPaged({
                pageSize: 1000
            });

            var page, transactionsArrayPC = [];
            var ctasXPagarJson = {};
            var cont = 0;

            pagedData.pageRanges.forEach(function(pageRange) {
                page = pagedData.fetch({
                    index: pageRange.index
                });
                page.data.forEach(function(result) {
                    cont++;
                    var rowArray = [];
                    var saldo = Math.abs(Number(result.getValue(result.columns[2])));
                    if (saldo != 0) {
                        // 0.tipo
                        if (result.getValue(result.columns[0]) != '- None -' && result.getValue(result.columns[0]) != '' && result.getValue(result.columns[0]) != null) {
                            rowArray[0] = result.getValue(result.columns[0]);
                        } else {
                            rowArray[0] = "";
                        }
                        // 1.cuenta
                        if (result.getValue(result.columns[1]) != '- None -' && result.getValue(result.columns[1]) != '' && result.getValue(result.columns[1]) != null) {
                            rowArray[1] = jsonArr[result.getValue(result.columns[1])] || "0";
                        } else {
                            rowArray[1] = "0";
                        }
                        // 2. SALDOS CUENTAS POR PAGAR
                        if (result.getValue(result.columns[2]) != '- None -' && result.getValue(result.columns[2]) != '' && result.getValue(result.columns[2]) != null) {
                            rowArray[2] = Number(result.getValue(result.columns[2]));
                        } else {
                            rowArray[2] = 0;
                        }
                        // 3. VENDOR
                        if (result.getValue(result.columns[3]) != '- None -' && result.getValue(result.columns[3]) != '' && result.getValue(result.columns[3]) != null) {
                            rowArray[3] = result.getValue(result.columns[3])
                        } else {
                            rowArray[3] = '';
                        }
                        // 4. CUSTOMER
                        if (result.getValue(result.columns[4]) != '- None -' && result.getValue(result.columns[4]) != '' && result.getValue(result.columns[4]) != null) {
                            rowArray[4] = result.getValue(result.columns[4])
                        } else {
                            rowArray[4] = '';
                        }
                        // 5. ENTITY
                        if (result.getValue(result.columns[5]) != '- None -' && result.getValue(result.columns[5]) != '' && result.getValue(result.columns[5]) != null) {
                            rowArray[5] = result.getValue(result.columns[5])
                        } else {
                            rowArray[5] = '';
                        }
                        rowArray[6] = 'MediosMagneticos';

                        var idEntity = ObtenerIdEntidad(rowArray[0], rowArray[3], rowArray[4], rowArray[5]) || ['- None -'];

                        var key = rowArray[1] + '|' + idEntity[0];
                        //log.error('key ', key);
                        //log.error("rowArray", rowArray);
                        if (ctasXPagarJson[key] === undefined) {
                            ctasXPagarJson[key] = rowArray;
                        } else {
                            //log.error('encontro ', key);
                            ctasXPagarJson[key][2] = round(ctasXPagarJson[key][2] + rowArray[2]);
                        }


                    }
                });
            });
            //log.error("ctasXPagarJson", ctasXPagarJson);
            //transactionsArrayPC = Object.values(ctasXPagarJson);
            log.error("cont ctasXPagar", cont);
            for (key in ctasXPagarJson) {
                if (Number((Math.abs(ctasXPagarJson[key][2])).toFixed(0)) != 0) {
                    transactionsArrayPC.push(ctasXPagarJson[key]);
                }
            }

            return transactionsArrayPC;
        }

        function ObtenerIdEntidad(transactionType, vendorId, customerId, entityId) {

            var entidad, internalId;

            if (vendorId != '' || customerId != '' || entityId != '') {

                if (transactionType == 'VendBill' || transactionType == 'VendCred' || transactionType == 'ItemRcpt' || transactionType == 'CardChrg') {
                    entidad = 'vendor';
                    internalId = vendorId;
                } else if (transactionType == 'CustInvc' || transactionType == 'CustCred' || transactionType == 'CustPymt' || transactionType == 'CashSale') {
                    entidad = 'customer';
                    internalId = customerId;
                } else if (transactionType == 'Journal' || transactionType == 'ExpRept' || transactionType == 'FxReval' || transactionType == 'VendPymt') {
                    if (vendorId != '') {
                        entidad = 'vendor';
                        internalId = vendorId;
                    } else if (customerId != '') {
                        entidad = 'customer';
                        internalId = customerId;
                    } else {
                        var entitySearchObj = search.create({
                            type: "entity",
                            filters: [
                                ["internalid", "anyof", entityId]
                            ],
                            columns: [
                                search.createColumn({
                                    name: "formulatext",
                                    formula: "{type.id}",
                                    label: "Formula (Text)"
                                }),
                                search.createColumn({
                                    name: "internalid",
                                    label: "Internal ID"
                                })
                            ]
                        });
                        var ObjEntity = entitySearchObj.run().getRange(0, 10);
                        if (ObjEntity.length != 0) {
                            var columns = ObjEntity[0].columns;
                            if (ObjEntity[0].getValue(columns[0]) == 'Vendor') {
                                var entidad = 'vendor';
                            } else if (ObjEntity[0].getValue(columns[0]) == 'CustJob') {
                                var entidad = 'customer';
                            } else if (ObjEntity[0].getValue(columns[0]) == 'Employee') {
                                var entidad = 'employee';
                            }
                            internalId = entityId;
                        } else {
                            return '';
                        }

                    }

                } else if (transactionType == 'Check') {

                    if (vendorId != '') {
                        internalId = vendorId;
                    } else if (entityId != '') {
                        internalId = entityId;
                    }

                    var entitySearchObj = search.create({
                        type: "entity",
                        filters: [
                            ["internalid", "anyof", internalId]
                        ],
                        columns: [
                            search.createColumn({
                                name: "formulatext",
                                formula: "{type.id}",
                                label: "Formula (Text)"
                            }),
                            search.createColumn({
                                name: "internalid",
                                label: "Internal ID"
                            })
                        ]
                    });
                    var ObjEntity = entitySearchObj.run().getRange(0, 10);
                    if (ObjEntity.length != 0) {
                        var columns = ObjEntity[0].columns;
                        if (ObjEntity[0].getValue(columns[0]) == 'Vendor') {
                            var entidad = 'vendor';
                        } else if (ObjEntity[0].getValue(columns[0]) == 'CustJob') {
                            var entidad = 'customer';
                        } else if (ObjEntity[0].getValue(columns[0]) == 'Employee') {
                            var entidad = 'employee';
                        }
                    } else {
                        return '';
                    }
                } else {
                    if (vendorId != '') {
                        entidad = 'vendor';
                        internalId = vendorId;
                    } else if (customerId != '') {
                        entidad = 'customer';
                        internalId = customerId;
                    } else {
                        var entitySearchObj = search.create({
                            type: "entity",
                            filters: [
                                ["internalid", "anyof", entityId]
                            ],
                            columns: [
                                search.createColumn({
                                    name: "formulatext",
                                    formula: "{type.id}",
                                    label: "Formula (Text)"
                                }),
                                search.createColumn({
                                    name: "internalid",
                                    label: "Internal ID"
                                })
                            ]
                        });
                        var ObjEntity = entitySearchObj.run().getRange(0, 10);
                        if (ObjEntity.length != 0) {
                            var columns = ObjEntity[0].columns;
                            if (ObjEntity[0].getValue(columns[0]) == 'Vendor') {
                                var entidad = 'vendor';
                            } else if (ObjEntity[0].getValue(columns[0]) == 'CustJob') {
                                var entidad = 'customer';
                            } else if (ObjEntity[0].getValue(columns[0]) == 'Employee') {
                                var entidad = 'employee';
                            }
                            internalId = entityId;
                        } else {
                            return '';
                        }
                    }
                }
                return [internalId, entidad];

            } else {
                return '';
            }
        }

        function getInformationCuentasxPagar(objResult) {

            var idEntity = ObtenerIdEntidad(objResult[0], objResult[3], objResult[4], objResult[5]) || ['- None -'];
            //log.error('idEntity', idEntity);
            var arrCuentas_x_Pagar = [];

            if (idEntity.length != 0) {
                //0. Concepto
                arrCuentas_x_Pagar[0] = objResult[1];

                //1. - 12. Info
                var internalId = idEntity[0];
                var entidad = idEntity[1];

                if (internalId != '- None -') {
                    if (entidad == 'employee') {
                        var aux_array = getInformationEmploy(entidad, internalId);
                    } else {
                        //log.debug("Entidad q no es employee", entidad);
                        //log.debug("Entidad q no es employee internalid", internalId);
                        var aux_array = getInformation(entidad, internalId);
                    }
                } else {
                    var aux_array = ['', '', '', '', '', '', '', '', '', '', '', ''];
                }

                arrCuentas_x_Pagar = arrCuentas_x_Pagar.concat(aux_array);

                //13. Saldo Cuentas x Pagar

                arrCuentas_x_Pagar[13] = objResult[2];

                return arrCuentas_x_Pagar;
            }
        }

        function ObtenerConceptos20032004(jsonACC) {

            var jsonBank = ObtenerBancos();
            var accounts2203_04 = Object.keys(jsonACC);
            log.debug('jsonACC ', accounts2203_04);
            log.debug('jsonBank ', jsonBank);
            var transactionsArray = [];
            var infoTxt = '';
            var contarRegistros = 0;

            /*LatamReady - CO Form 1009 Balance of Accounts Payable 2203 and 2204 V7.1*/
            var savedSearch = search.load({
                id: 'customsearch_lmry_co_form_1009_0304_v71'
            });

            if (paramPeriodo) {

                var periodEndDate = new Date(paramPeriodo, 11, 31);

                periodEndDate = format.format({
                    value: periodEndDate,
                    type: format.Type.DATE
                });

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

                if (accounts2203_04.length != 0) {

                    var filter2203_2204 = search.createFilter({
                        name: 'account',
                        join: 'accountingtransaction',
                        operator: search.Operator.ANYOF,
                        values: accounts2203_04
                    });
                    savedSearch.filters.splice(3, 1, filter2203_2204);

                } else {
                    log.debug("accounts2203_04 es 0", "accounts2203_04 es 0");

                    var filter2203_2204 = search.createFilter({
                        name: 'account',
                        join: 'accountingtransaction',
                        operator: search.Operator.ANYOF,
                        values: ['-1']
                    });
                    savedSearch.filters.push(filter2203_2204);
                }
                //2.-
                var column13 = search.createColumn({
                    name: 'formulacurrency',
                    summary: 'SUM',
                    formula: "-(NVL({accountingtransaction.debitamount},0)-NVL({accountingtransaction.creditamount},0))"
                });
                savedSearch.columns.splice(2, 1, column13); //Esta eliminando 1 y agregando el 13
                //0.-
                var accountIdColumn = search.createColumn({
                    name: 'formulanumeric',
                    summary: 'GROUP',
                    formula: '{accountingtransaction.account.id}',
                });
                savedSearch.columns.splice(0, 1, accountIdColumn); //Esta eliminando 1 y agregando el accountId

            } else {

                if (accounts2203_04.length != 0) {

                    var filter2203_2204 = search.createFilter({
                        name: 'account',
                        operator: search.Operator.ANYOF,
                        values: accounts2203_04
                    });
                    savedSearch.filters.splice(3, 1, filter2203_2204);


                } else {
                    log.debug("accounts2203_04 es 0", "accounts2203_04 es 0");

                    var filter2203_2204 = search.createFilter({
                        name: 'account',
                        operator: search.Operator.ANYOF,
                        values: ['-1']
                    });
                    savedSearch.filters.push(filter2203_2204);
                }
                //Esta cambiando el account id de la primera columna
                var accountIdColumn = search.createColumn({
                    name: 'formulanumeric',
                    summary: 'GROUP',
                    formula: '{account.id}',
                });
                savedSearch.columns.splice(0, 1, accountIdColumn);

            }
            var pagedData = savedSearch.runPaged({
                pageSize: 1000
            });

            var page, transactionsArray = [];
            var cont = 0;
            var ctasXPagarJson = {};

            pagedData.pageRanges.forEach(function(pageRange) {
                page = pagedData.fetch({
                    index: pageRange.index
                });
                page.data.forEach(function(result) {
                    cont++;
                    var rowArray = [];
                    var saldo = Math.abs(Number(result.getValue(result.columns[2])));
                    if (saldo != 0) {

                        // 0.CUENTA
                        if (result.getValue(result.columns[0]) != '- None -' && result.getValue(result.columns[0]) != '' && result.getValue(result.columns[0]) != null) {
                            //log.error('cuenta ', result.getValue(result.columns[0]));
                            rowArray[0] = jsonACC[result.getValue(result.columns[0])][0];
                        } else {
                            rowArray[0] = '';
                        }
                        // 1.BANK NAME
                        if (result.getValue(result.columns[0]) != '- None -' && result.getValue(result.columns[0]) != '' && result.getValue(result.columns[0]) != null) {
                            var name_bank = jsonACC[result.getValue(result.columns[0])][1];
                            //log.error("name_bank", name_bank);
                            var arrbank = jsonBank[name_bank] || [];
                            if (arrbank.length) {
                                rowArray[1] = arrbank[5];
                                rowArray[2] = RetornaNumero(arrbank[8]);
                                rowArray[3] = RecortarCaracteres(arrbank[7], 1);
                                rowArray[4] = '';
                                rowArray[5] = '';
                                rowArray[6] = '';
                                rowArray[7] = '';
                                rowArray[8] = arrbank[9];
                                rowArray[9] = arrbank[10];
                                rowArray[10] = arrbank[11];
                                rowArray[11] = arrbank[12];
                                rowArray[12] = arrbank[6];
                            } else {
                                rowArray.concat(['', '', '', '', '', '', '', '', '', '', '', '']);
                            }
                        } else {
                            rowArray.concat(['', '', '', '', '', '', '', '', '', '', '', '']);
                        }
                        // 13. SALDOS CUENTAS POR PAGAR
                        if (result.getValue(result.columns[2]) != '- None -' && result.getValue(result.columns[2]) != '' && result.getValue(result.columns[2]) != null) {
                            rowArray[13] = Number(result.getValue(result.columns[2]));
                        } else {
                            rowArray[13] = 0;
                        }
                        rowArray[14] = 'Bank_concept';

                        var key = rowArray[0] + '|' + name_bank;

                        log.debug('rowArray bancos', rowArray);

                        if (ctasXPagarJson[key] === undefined) {
                            ctasXPagarJson[key] = rowArray;
                        } else {
                            //log.error('encontro ', key);
                            ctasXPagarJson[key][13] = round(ctasXPagarJson[key][13] + rowArray[13]);
                        }
                    }
                });
            });

            log.debug('cont ctasXPagar Bancos', cont);

            for (key in ctasXPagarJson) {
                transactionsArray.push(ctasXPagarJson[key])
            }
            return transactionsArray;
        }

        function ObtenerBancos() {

            var rowArray = [];
            var json_bank = {};
            var objKey;

            var savedSearch = search.load({
                id: 'customsearch_lmry_co_bank'
            });
            //10.- address
            var addressBank = search.createColumn({
                name: 'formulatext',
                formula: '{custrecord_lmry_bank_direccion}',
            });
            savedSearch.columns.push(addressBank);

            //11.- departamento - prov id
            var departmentBank = search.createColumn({
                name: 'formulatext',
                formula: '{custrecord_lmry_bank_department.custrecord_lmry_prov_id}',
            });
            savedSearch.columns.push(departmentBank);

            //12.- municipio - cod municipio
            var codeMunicipioBank = search.createColumn({
                name: 'formulatext',
                formula: '{custrecord_lmry_bank_municipality.custrecord_lmry_city_id}',
            });
            savedSearch.columns.push(codeMunicipioBank);


            var searchresult = savedSearch.run();
            var objResult = searchresult.getRange(0, 1000);
            if (objResult != null) {

                for (var i = 0; i < objResult.length; i++) {

                    objKey = '';
                    columns = objResult[i].columns;
                    rowArray = [];

                    //O. NOMBRE
                    if (objResult[i].getValue(columns[0]) != null) {
                        rowArray[0] = objResult[i].getValue(columns[0]);
                        objKey = rowArray[0];
                    } else {
                        rowArray[0] = '';
                    }
                    //1. ID
                    if (objResult[i].getValue(columns[1]) != null)
                        rowArray[1] = objResult[i].getValue(columns[1]);
                    else
                        rowArray[1] = '';
                    //2. NOMBRE CORTO
                    if (objResult[i].getValue(columns[2]) != null)
                        rowArray[2] = objResult[i].getValue(columns[2]);
                    else
                        rowArray[2] = '';
                    //3. CLAVE
                    if (objResult[i].getValue(columns[3]) != null)
                        rowArray[3] = objResult[i].getValue(columns[3]);
                    else
                        rowArray[3] = '';
                    //4. PAIS
                    if (objResult[i].getValue(columns[4]) != null)
                        rowArray[4] = objResult[i].getValue(columns[4]);
                    else
                        rowArray[4] = '';
                    //5. CO TDOC
                    if (objResult[i].getValue(columns[5]) != null)
                        rowArray[5] = objResult[i].getValue(columns[5]);
                    else
                        rowArray[5] = '';
                    //6. CODIGO PAIS
                    if (objResult[i].getValue(columns[6]) != null)
                        rowArray[6] = objResult[i].getValue(columns[6]);
                    else
                        rowArray[6] = '';
                    //7. DV
                    if (objResult[i].getValue(columns[7]) != null)
                        rowArray[7] = objResult[i].getValue(columns[7]);
                    else
                        rowArray[7] = '';
                    //8. NID
                    if (objResult[i].getValue(columns[8]) != null)
                        rowArray[8] = objResult[i].getValue(columns[8]);
                    else
                        rowArray[8] = '';
                    //9. RAZON SOCIAL
                    if (objResult[i].getValue(columns[9]) != null)
                        rowArray[9] = objResult[i].getValue(columns[9]);
                    else
                        rowArray[9] = '';

                    //10. Direccion
                    if (objResult[i].getValue(columns[10]) != null)
                        rowArray[10] = objResult[i].getValue(columns[10]);
                    else
                        rowArray[10] = '';
                    //11. Department
                    if (objResult[i].getValue(columns[11]) != null)
                        rowArray[11] = objResult[i].getValue(columns[11]);
                    else
                        rowArray[11] = '';
                    //12. Municipalidad
                    if (objResult[i].getValue(columns[12]) != null)
                        rowArray[12] = objResult[i].getValue(columns[12]).slice(-3);
                    else
                        rowArray[12] = '';


                    json_bank[objKey] = rowArray;
                }
                //log.error('json_bank', json_bank);
                return json_bank;
            }
        }

        function getInformation(entityType, entityId) {

            //log.error("entityType", entityType + ' - ' + entityId);

            var auxArray = [];

            if (entityType != '' && entityId != '') {

                var newSearch = search.create({
                    type: entityType,
                    filters: [
                        ['internalid', 'is', entityId],
                        "AND", ["isdefaultbilling", "is", "T"]
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
                            formula: "CASE WHEN {custentity_lmry_sunat_tipo_doc_id} = 'NIT' THEN {custentity_lmry_digito_verificator} ELSE '' END",
                            label: "2. Digito Verificador"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "CASE WHEN {isperson} = 'T' THEN {lastname} ELSE '' END",
                            label: "3. Apellidos"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "CASE WHEN {isperson} = 'T' THEN {firstname} ELSE '' END",
                            label: "4. Nombres"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "CASE WHEN {isperson} = 'F' THEN {companyname} ELSE '' END",
                            label: "5. Razón Social"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "{address1}",
                            label: "6. Dirección"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "{custentity_lmry_municcode}",
                            label: "7. Departamento y Municipio"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "{custentity_lmry_country}",
                            label: "8. Pais"
                        })
                    ]
                });

                var objResult = newSearch.run().getRange(0, 1000);
                if (objResult && objResult.length) {
                    var columns = objResult[0].columns;

                    // 0. Tipo de Documento
                    auxArray[0] = objResult[0].getValue(columns[0]);

                    // 1. NIT
                    auxArray[1] = QuitarCaracteres(objResult[0].getValue(columns[1]));

                    // 2. DV
                    auxArray[2] = objResult[0].getValue(columns[2]);

                    // 3. Apellido Paterno
                    if (objResult[0].getValue(columns[3]).split(' ')[0]) {
                        auxArray[3] = ValidarCaracteres_Especiales(objResult[0].getValue(columns[3]).split(' ')[0]);
                    } else {
                        auxArray[3] = '';
                    }

                    // 4. Apellido Materno
                    if (objResult[0].getValue(columns[3]).split(' ')[1]) {
                        auxArray[4] = ValidarCaracteres_Especiales(objResult[0].getValue(columns[3]).split(' ')[1]);
                    } else {
                        auxArray[4] = '';
                    }

                    // 5. Primer Nombre
                    if (objResult[0].getValue(columns[4]).split(' ')[0]) {
                        auxArray[5] = ValidarCaracteres_Especiales(objResult[0].getValue(columns[4]).split(' ')[0]);
                    } else {
                        auxArray[5] = '';
                    }

                    // 6. Segundo Nombre
                    if (objResult[0].getValue(columns[4]).split(' ')[1]) {
                        auxArray[6] = ValidarCaracteres_Especiales(objResult[0].getValue(columns[4]).split(' ')[1]);
                    } else {
                        auxArray[6] = '';
                    }

                    // 7. Razón Social
                    auxArray[7] = ValidarCaracteres_Especiales(objResult[0].getValue(columns[5]));

                    // 8. Dirección
                    auxArray[8] = ValidarCaracteres_Especiales(objResult[0].getValue(columns[6]));

                    if (objResult[0].getValue(columns[7])) {
                        // 9. Código de Departamento
                        auxArray[9] = objResult[0].getValue(columns[7]).substring(0, 2);

                        // 10. Municipio
                        auxArray[10] = objResult[0].getValue(columns[7]).substring(2, 5);
                    } else {
                        // 9. Código de Departamento
                        auxArray[9] = '';

                        // 10. Municipio
                        auxArray[10] = '';
                    }

                    // 11. País
                    auxArray[11] = BuscarPais(objResult[0].getValue(columns[8]));

                } else {
                    auxArray = ['', '', '', '', '', '', '', '', '', '', '', ''];
                }
            }

            return auxArray;
        }

        function getInformationEmploy(entityType, entityId) {

            var auxArray = [];

            if (entityType != '' && entityId != '') {

                var newSearch = search.create({
                    type: entityType,
                    filters: [
                        ['internalid', 'is', entityId]
                    ],
                    columns: [
                        search.createColumn({
                            name: "formulatext",
                            formula: "{custentity_lmry_sunat_tipo_doc_cod}",
                            label: "0. Tipo de Documento"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "{custentity_lmry_sv_taxpayer_number}",
                            label: "1. NIT"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "CASE WHEN {custentity_lmry_sunat_tipo_doc_id} = 'NIT' THEN {custentity_lmry_digito_verificator} ELSE '' END",
                            label: "2. Digito Verificador"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "{lastname}",
                            label: "3. Apellidos"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "{firstname}",
                            label: "4. Nombres"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "' '",
                            label: "5. Razón Social"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "{address1}",
                            label: "6. Dirección"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "{address.custrecord_lmry_addr_prov_id}",
                            label: "7.  LATAM - PROVINCE ID"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "{address.custrecord_lmry_addr_city_id}",
                            label: "8.  LATAM - CITY ID"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "{address.country}",
                            label: "9. PAIS"
                        })

                    ]
                });

                var objResult = newSearch.run().getRange(0, 1000);
                if (objResult && objResult.length) {
                    var columns = objResult[0].columns;

                    // 0. Tipo de Documento
                    auxArray[0] = objResult[0].getValue(columns[0]);

                    // 1. NIT
                    auxArray[1] = QuitarCaracteres(objResult[0].getValue(columns[1]));

                    // 2. DV
                    auxArray[2] = objResult[0].getValue(columns[2]);

                    // 3. Apellido Paterno
                    if (objResult[0].getValue(columns[3]).split(' ')[0]) {
                        auxArray[3] = ValidarCaracteres_Especiales(objResult[0].getValue(columns[3]).split(' ')[0]);
                    } else {
                        auxArray[3] = '';
                    }

                    // 4. Apellido Materno
                    if (objResult[0].getValue(columns[3]).split(' ')[1]) {
                        auxArray[4] = ValidarCaracteres_Especiales(objResult[0].getValue(columns[3]).split(' ')[1]);
                    } else {
                        auxArray[4] = '';
                    }


                    // 5. Primer Nombre
                    if (objResult[0].getValue(columns[4]).split(' ')[0]) {
                        auxArray[5] = ValidarCaracteres_Especiales(objResult[0].getValue(columns[4]).split(' ')[0]);
                    } else {
                        auxArray[5] = '';
                    }

                    // 6. Segundo Nombre
                    if (objResult[0].getValue(columns[4]).split(' ')[1]) {
                        auxArray[6] = ValidarCaracteres_Especiales(objResult[0].getValue(columns[4]).split(' ')[1]);
                    } else {
                        auxArray[6] = '';
                    }

                    // 7. Razón Social
                    auxArray[7] = ValidarCaracteres_Especiales(objResult[0].getValue(columns[5]));

                    // 8. Dirección
                    auxArray[8] = ValidarCaracteres_Especiales(objResult[0].getValue(columns[6]));

                    if (objResult[0].getValue(columns[6])) {
                        // 9. Código de Departamento
                        auxArray[9] = objResult[0].getValue(columns[7]);

                        // 10. Municipio
                        auxArray[10] = objResult[0].getValue(columns[8]);
                    } else {
                        // 9. Código de Departamento
                        auxArray[9] = '';

                        // 10. Municipio
                        auxArray[10] = '';
                    }

                    // 11. País
                    auxArray[11] = BuscarPais(objResult[0].getValue(columns[9]));
                } else {
                    auxArray = ['', '', '', '', '', '', '', '', '', '', '', ''];
                }
            }
            return auxArray;
        }

        function getCheckEntityDetail(objResult) {

            //log.error('Entro al ', 'getCheckEntityDetail');

            var arrCheck = objResult;
            var account = arrCheck[2];

            var newSearch = search.create({
                type: 'account',
                filters: [
                    ['internalid', 'anyof', account], 'AND', ["custrecord_lmry_co_puc_concept", "noneof", "46", "27"]
                ],
                columns: [
                    search.createColumn({
                        name: "formulanumeric",
                        formula: "{internalid}",
                        label: "0. Account Id"
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "{custrecord_lmry_co_puc_concept.custrecord_lmry_co_puc_number_c}",
                        label: "1. Format Number"
                    })
                ]
            });

            var objResult_2 = newSearch.run().getRange(0, 1000);

            var resultArray = [],
                key = '',
                accountsDetailJson = {};

            if (objResult_2 && objResult_2.length) {

                for (var j = 0; j < objResult_2.length; j++) {
                    resultArray = [];
                    columns = objResult_2[j].columns;

                    // Concepto
                    resultArray[0] = objResult_2[j].getValue(columns[1]);

                    var aux_info = getInformation('customer', arrCheck[0]);

                    if (aux_info.length != 0) {

                        resultArray = resultArray.concat(aux_info);
                        resultArray[13] = arrCheck[2];

                        //log.error('resultArray', resultArray);

                        if (!(arrCheck[1] == 0)) {

                            key = resultArray.slice(0, 13).join('|');

                            if (accountsDetailJson[key] === undefined) {
                                accountsDetailJson[key] = resultArray;
                            } else {
                                accountsDetailJson[key][13] += resultArray[13];
                            }
                        }
                    }
                }
                return accountsDetailJson;
            }
        }



        function ObtenerCuentas() {
            var accountJson_2203_2204 = {};
            var accountJson = {};
            var savedSearch = search.create({
                type: 'account',
                filters: [
                    ["custrecord_lmry_co_puc_formatgy", "anyof", "7"],
                    "AND", ["custrecord_lmry_co_puc_concept.custrecord_lmry_co_puc_formatid_c", "is", "1009"]
                ],
                columns: [
                    search.createColumn({
                        name: 'internalid'
                    }),
                    search.createColumn({
                        name: 'formulatext',
                        formula: 'SUBSTR({custrecord_lmry_co_puc_concept.name}, 1,4)'
                    }),
                    search.createColumn({
                        name: 'formulatext',
                        formula: '{custrecord_lmry_bank_name}'
                    })
                ]
            });

            var objResult = savedSearch.run().getRange(0, 1000);

            if (objResult && objResult.length) {
                var columns;
                for (var i = 0; i < objResult.length; i++) {
                    columns = objResult[i].columns;
                    if (objResult[i].getValue(columns[1]) == "2203" || objResult[i].getValue(columns[1]) == "2204") {
                        accountJson_2203_2204[objResult[i].getValue(columns[0])] = [objResult[i].getValue(columns[1]), objResult[i].getValue(columns[2])];
                    } else {
                        accountJson[objResult[i].getValue(columns[0])] = objResult[i].getValue(columns[1]);
                    }
                }
            }
            return [accountJson, accountJson_2203_2204];

        }

        function ObtenerNumeroEnvio() {

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


        function BuscarPais(pais) {

            var paisesArray = ObtenerPaises();

            for (var i = 0; i < paisesArray.length; i++) {
                if (pais == paisesArray[i][0]) {
                    return paisesArray[i][2];
                }
            }
            return '';
        }

        function ObtenerPaises() {

            var paisesArray_aux = [];

            var savedSearch = search.create({
                type: 'customrecord_lmry_mx_country',
                filters: [
                    ["isinactive", "is", "F"],
                    "AND", ["custrecord_country_localization", "anyof", "48"]
                ],
                columns: [
                    'name',
                    'custrecord_lmry_mx_country',
                    'custrecord_lmry__mx_contrycode',
                    'custrecord_lmry__mx_contrycitizen',
                    'custrecord_country_localization'
                ]
            });

            var searchResult = savedSearch.run();
            var objResult;
            var columns;


            objResult = searchResult.getRange(0, 1000);

            if (objResult != null) {

                for (var i = 0; i < objResult.length; i++) {
                    columns = objResult[i].columns;
                    rowArray = [];

                    if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -') {
                        rowArray[0] = objResult[i].getValue(columns[0]);
                    } else {
                        rowArray[0] = '';
                    }

                    if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -') {
                        rowArray[1] = objResult[i].getText(columns[1]);
                    } else {
                        rowArray[1] = '';
                    }

                    if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -') {
                        rowArray[2] = objResult[i].getValue(columns[2]);
                    } else {
                        rowArray[2] = '';
                    }

                    if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -') {
                        rowArray[3] = objResult[i].getValue(columns[3]);
                    } else {
                        rowArray[3] = '';
                    }

                    if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -') {
                        rowArray[4] = objResult[i].getValue(columns[4]);
                    } else {
                        rowArray[4] = '';
                    }

                    paisesArray_aux.push(rowArray);
                }
                return paisesArray_aux;
            }

        }

        function RetornaNumero(nid) {
            if (nid != null && nid != '') {
                return nid.replace(/(\.|\-)/g, '');
            }
            return '';
        }

        function RecortarCaracteres(valor, numero) {
            if (valor != null && valor.length > numero) {
                return valor.substring(0, numero);
            }
            return valor;
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

        function RetornaNumero(nid) {
            if (nid != null && nid != '') {
                return nid.replace(/(\.|\-)/g, '');
            }
            return '';
        }

        function round(number) {
            return Math.round(Number(number) * 100) / 100;
        }

        function obtenerSubsidiariaAddress() {
            var subsidiaryRecord = search.lookupFields({
                type: search.Type.SUBSIDIARY,
                id: paramSubsidiaria,
                columns: ['address.address1', 'address.custrecord_lmry_addr_prov_id', 'address.custrecord_lmry_addr_city_id', 'address.country']
            });
            var auxAddress = [];
            //log.debug("subsidiaryRecord", subsidiaryRecord);
            auxAddress[0] = subsidiaryRecord["address.address1"];
            auxAddress[1] = subsidiaryRecord["address.custrecord_lmry_addr_prov_id"];
            auxAddress[2] = subsidiaryRecord["address.custrecord_lmry_addr_city_id"];
            //log.debug("valor de address.country", subsidiaryRecord["address.country"][0].value);
            auxAddress[3] = BuscarPais(subsidiaryRecord["address.country"][0].value);
            //log.debug("valor de auxAddress[3]", auxAddress[3]);

            return auxAddress;
        }

        function NoData() {

            log.debug("no data", 'no data');
            var usuario = runtime.getCurrentUser();

            var generarXml = false;

            if (paramIdReport) {
                var report = search.lookupFields({
                    type: 'customrecord_lmry_co_features',
                    id: paramIdReport,
                    columns: ['name']
                });
                reportName = report.name;
            }

            if (isSubsidiariaFeature) {
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


            if (isMultibookFeature) {
                var multibookName = search.lookupFields({
                    type: search.Type.ACCOUNTING_BOOK,
                    id: paramMultibook,
                    columns: ['name']
                }).name;
            }

            var employee = search.lookupFields({
                type: search.Type.EMPLOYEE,
                id: usuario.id,
                columns: ['firstname', 'lastname']
            });
            var usuarioName = employee.firstname + ' ' + employee.lastname;

            if (generarXml) {
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
                value: paramPeriodo
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


        /**********************************************************************
         * Generacion de estructura excel y XML
         **********************************************************************/

        function GenerarExcel(ctasXPagarArray, numeroEnvio) {

            if (isSubsidiariaFeature) {
                var subsidiaryRecord = search.lookupFields({
                    type: search.Type.SUBSIDIARY,
                    id: paramSubsidiaria,
                    columns: ['legalname', 'taxidnum']
                });
                var companyName = subsidiaryRecord.legalname;
                var companyRuc = subsidiaryRecord.taxidnum;
            } else {
                var pageConfig = config.load({
                    type: config.Type.COMPANY_INFORMATION
                });
                var companyName = pageConfig.getValue('legalname');
                var companyRuc = pageConfig.getValue('employerid');
            }
            log.debug('companyName', companyName);
            companyName = ValidarCaracteres_Especiales(companyName);
            log.debug('companyName valid', companyName);

            if (isMultibookFeature) {
                var multibookName = search.lookupFields({
                    type: search.Type.ACCOUNTING_BOOK,
                    id: paramMultibook,
                    columns: ['name']
                }).name;
            }

            var periodStartDate = "01/01/" + paramPeriodo;

            var periodEndDate = "31/12/" + paramPeriodo;

            //PDF Normalization
            var todays = parseDateTo(new Date(), "DATE");
            var currentTime = getTimeHardcoded(parseDateTo(new Date(), "DATETIME"));

            var xlsString = '';

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
            xlsString += '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['titulo'][language] + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row></Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['razonSocial'][language] + ': ' + companyName + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['taxNumber'][language] + ': ' + companyRuc + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['periodo'][language] + ': ' + GLOBAL_LABELS['al'][language] + ' ' + periodEndDate + '</Data></Cell>';
            xlsString += '</Row>';
            if (isMultibookFeature) {
                xlsString += '<Row>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell><Data ss:Type="String">Multibook: ' + multibookName + '</Data></Cell>';
                xlsString += '</Row>';
            }

            // PDF Normalized

            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['origin'][language] + ': Netsuite' + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['date'][language] + ': ' + todays + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell><Data ss:Type="String">' + GLOBAL_LABELS['time'][language] + ': ' + currentTime + '</Data></Cell>';
            xlsString += '</Row>';

            // END PDF Normalized

            xlsString += '<Row></Row>';
            xlsString += '<Row></Row>';
            xlsString += '<Row>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String">' + GLOBAL_LABELS['concepto'][language] + '</Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> TDOC </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> NID </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> D.V. </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['primerApellido'][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['segApellido'][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['primerNombre'][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['segNombre'][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['razonSocial'][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['direccion'][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['departamento'][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> M/PIO </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['pais'][language] + ' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> ' + GLOBAL_LABELS['saldos'][language] + ' CtaxPagar </Data></Cell>' +
                '</Row>';


            //creacion de reporte xls
            for (var i = 0; i < ctasXPagarArray.length; i++) {

                if (Number(Math.abs(ctasXPagarArray[i][13]).toFixed()) > 0) {

                    xlsString += '<Row>';
                    // 0.CONCEPTO
                    if (ctasXPagarArray[i][0] != null && ctasXPagarArray[i][0] != 'null' && ctasXPagarArray[i][0] != '- None -' && ctasXPagarArray[i][0] != '') {
                        xlsString += '<Cell><Data ss:Type="String">' + ctasXPagarArray[i][0] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String">' + '' + '</Data></Cell>';
                    }

                    // 1.Tipo de Documento
                    if (ctasXPagarArray[i][1] != null && ctasXPagarArray[i][1] != 'null' && ctasXPagarArray[i][1] != '- None -' && ctasXPagarArray[i][1] != '') {
                        xlsString += '<Cell><Data ss:Type="String">' + ctasXPagarArray[i][1] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String">' + '' + '</Data></Cell>';
                    }

                    // 2.NID
                    if (ctasXPagarArray[i][2] != null && ctasXPagarArray[i][2] != 'null' && ctasXPagarArray[i][2] != '- None -' && ctasXPagarArray[i][2] != '') {
                        xlsString += '<Cell><Data ss:Type="String">' + ctasXPagarArray[i][2] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String">' + '' + '</Data></Cell>';
                    }

                    // 3.Digito Verificador
                    if (ctasXPagarArray[i][3] != null && ctasXPagarArray[i][3] != 'null' && ctasXPagarArray[i][3] != '- None -' && ctasXPagarArray[i][3] != '') {
                        xlsString += '<Cell><Data ss:Type="String">' + ctasXPagarArray[i][3] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String">' + '' + '</Data></Cell>';
                    }

                    // 4.1er Apellido
                    if (ctasXPagarArray[i][4] != null && ctasXPagarArray[i][4] != 'null' && ctasXPagarArray[i][4] != '- None -' && ctasXPagarArray[i][4] != '') {
                        xlsString += '<Cell><Data ss:Type="String">' + ctasXPagarArray[i][4] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String">' + '' + '</Data></Cell>';
                    }

                    // 5.2do Apellido
                    if (ctasXPagarArray[i][5] != null && ctasXPagarArray[i][5] != 'null' && ctasXPagarArray[i][5] != '- None -' && ctasXPagarArray[i][5] != '') {
                        xlsString += '<Cell><Data ss:Type="String">' + ctasXPagarArray[i][5] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String">' + '' + '</Data></Cell>';
                    }

                    // 6.1er Nombre
                    if (ctasXPagarArray[i][6] != null && ctasXPagarArray[i][6] != 'null' && ctasXPagarArray[i][6] != '- None -' && ctasXPagarArray[i][6] != '') {
                        xlsString += '<Cell><Data ss:Type="String">' + ctasXPagarArray[i][6] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String">' + '' + '</Data></Cell>';
                    }

                    // 7.2do Nombre
                    if (ctasXPagarArray[i][7] != null && ctasXPagarArray[i][7] != 'null' && ctasXPagarArray[i][7] != '- None -' && ctasXPagarArray[i][7] != '') {
                        xlsString += '<Cell><Data ss:Type="String">' + ctasXPagarArray[i][7] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String">' + '' + '</Data></Cell>';
                    }

                    // 8.Razon Social
                    if (ctasXPagarArray[i][8] != null && ctasXPagarArray[i][8] != 'null' && ctasXPagarArray[i][8] != '- None -' && ctasXPagarArray[i][8] != '') {
                        xlsString += '<Cell><Data ss:Type="String">' + ctasXPagarArray[i][8] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String">' + '' + '</Data></Cell>';
                    }

                    // 9.Direccion
                    if (ctasXPagarArray[i][9] != null && ctasXPagarArray[i][9] != 'null' && ctasXPagarArray[i][9] != '- None -' && ctasXPagarArray[i][9] != '') {
                        xlsString += '<Cell><Data ss:Type="String">' + ctasXPagarArray[i][9] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String">' + '' + '</Data></Cell>';
                    }

                    // 10.Codigo de Departamento
                    if (ctasXPagarArray[i][10] != null && ctasXPagarArray[i][10] != 'null' && ctasXPagarArray[i][10] != '- None -' && ctasXPagarArray[i][10] != '') {
                        xlsString += '<Cell><Data ss:Type="String">' + ctasXPagarArray[i][10] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String">' + '' + '</Data></Cell>';
                    }
                    // 11.Municipio
                    if (ctasXPagarArray[i][11] != null && ctasXPagarArray[i][11] != 'null' && ctasXPagarArray[i][11] != '- None -' && ctasXPagarArray[i][11] != '') {
                        xlsString += '<Cell><Data ss:Type="String">' + ctasXPagarArray[i][11] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String">' + '' + '</Data></Cell>';
                    }

                    // 12.Pais
                    if (ctasXPagarArray[i][12] != null && ctasXPagarArray[i][12] != 'null' && ctasXPagarArray[i][12] != '- None -' && ctasXPagarArray[i][12] != '') {
                        xlsString += '<Cell><Data ss:Type="String">' + ctasXPagarArray[i][12] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell><Data ss:Type="String">' + '' + '</Data></Cell>';
                    }

                    // 13.Saldos de Cuentas por Pagar
                    if (ctasXPagarArray[i][13] != null && ctasXPagarArray[i][13] != 'null' && ctasXPagarArray[i][13] != '- None -' && ctasXPagarArray[i][13] != '') {
                        xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + ctasXPagarArray[i][13] + '</Data></Cell>';
                    } else {
                        xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + '' + '</Data></Cell>';
                    }

                    xlsString += '</Row>';
                }
            }

            xlsString += '</Table></Worksheet></Workbook>';

            strExcelCtasXPagar = encode.convert({
                string: xlsString,
                inputEncoding: encode.Encoding.UTF_8,
                outputEncoding: encode.Encoding.BASE_64
            });

            SaveFile('.xls', strExcelCtasXPagar, numeroEnvio);
        }

        function GenerarXml(ctasXPagarArray, numeroEnvio, valorTotal) {
            var xmlString = '';

            var today = new Date();
            var anio = today.getFullYear();
            var mes = completar_cero(2, today.getMonth() + 1);
            var day = completar_cero(2, today.getDate());
            var hour = completar_cero(2, today.getHours());
            var min = completar_cero(2, today.getMinutes());
            var sec = completar_cero(2, today.getSeconds());
            today = anio + '-' + mes + '-' + day + 'T' + hour + ':' + min + ':' + sec;

            xmlString += '<?xml version="1.0" encoding="ISO-8859-1"?> \r\n';
            xmlString += '<mas xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"> \r\n';
            xmlString += '<Cab> \r\n';
            xmlString += '<Ano>' + paramPeriodo + '</Ano> \r\n';
            xmlString += '<CodCpt>' + paramConcepto + '</CodCpt> \r\n';
            xmlString += '<Formato>1009</Formato> \r\n';
            xmlString += '<Version>71</Version> \r\n';
            xmlString += '<NumEnvio>' + numeroEnvio + '</NumEnvio> \r\n';
            xmlString += '<FecEnvio>' + today + '</FecEnvio> \r\n';
            xmlString += '<FecFinal>' + paramPeriodo + '-12-31</FecFinal> \r\n';
            xmlString += '<ValorTotal>' + Math.abs(valorTotal).toFixed(0) + '</ValorTotal> \r\n';
            xmlString += '<CantReg>' + ctasXPagarArray.length + '</CantReg> \r\n';
            xmlString += '</Cab>\r\n';
            var val12, val11, val10;
            for (var i = 0; i < ctasXPagarArray.length; i++) {

                if (Number(Math.abs(ctasXPagarArray[i][13]).toFixed(2)) > 0) {

                    val12 = ctasXPagarArray[i][12];
                    val11 = ctasXPagarArray[i][11];
                    val10 = ctasXPagarArray[i][10];

                    if (val12 == null) {
                        val12 = '';
                    }
                    if (val11 == null) {
                        val11 = '';
                    }
                    if (val10 == null) {
                        val10 = '';
                    }

                    if (ctasXPagarArray[i][9]) {
                        xmlString += '<saldoscp sal="' + Number(ctasXPagarArray[i][13]).toFixed(0) + '" pais="' + val12 + '" mun="' + val11 + '" dpto="' + val10 + '" dir="' + ctasXPagarArray[i][9].replace(/&/g, '&amp;');
                    } else {
                        xmlString += '<saldoscp sal="' + Number(ctasXPagarArray[i][13]).toFixed(0) + '" pais="' + val12 + '" mun="' + val11 + '" dpto="' + val10 + '" dir="' + '';
                    }

                    if (ctasXPagarArray[i][7]) {
                        xmlString += '" nom2="' + ctasXPagarArray[i][7].replace(/&/g, '&amp;');
                    } else {
                        xmlString += '" nom2="';
                    }

                    if (ctasXPagarArray[i][6]) {
                        xmlString += '" nom1="' + ctasXPagarArray[i][6].replace(/&/g, '&amp;');
                    } else {
                        xmlString += '" nom1="';
                    }

                    if (ctasXPagarArray[i][5]) {
                        xmlString += '" apl2="' + ctasXPagarArray[i][5].replace(/&/g, '&amp;');
                    } else {
                        xmlString += '" apl2="';
                    }

                    if (ctasXPagarArray[i][4]) {
                        xmlString += '" apl1="' + ctasXPagarArray[i][4].replace(/&/g, '&amp;');
                    } else {
                        xmlString += '" apl1="';

                    }

                    if (ctasXPagarArray[i][8]) {
                        xmlString += '" raz="' + ctasXPagarArray[i][8].replace(/&/g, '&amp;');
                    } else {
                        xmlString += '" raz="';

                    }
                    var val3, val2, val1, val0;
                    val3 = ctasXPagarArray[i][3];
                    val2 = ctasXPagarArray[i][2];
                    val1 = ctasXPagarArray[i][1];
                    val0 = ctasXPagarArray[i][0];
                    if (val3 == null) {
                        val3 = '';
                    }
                    if (val2 == null) {
                        val2 = '';
                    }
                    if (val1 == null) {
                        val1 = '';
                    }
                    if (val0 == null) {
                        val0 = '';
                    }

                    xmlString += '" dv="' + val3 + '" nid="' + val2 + '" tdoc="' + val1 + '" cpt="' + val0 + '"/> \r\n';
                }
            }
            xmlString += '</mas> \r\n';

            strXmlCtasXPagar = xmlString;

            SaveFile('.xml', strXmlCtasXPagar, numeroEnvio);
        }

        function SaveFile(extension, strArchivo, numeroEnvio) {

            var numeroEnvio = numeroEnvio;
            //log.error("Entro al save File", 'wiiiii');
            var folderId = objContext.getParameter({
                name: 'custscript_lmry_file_cabinet_rg_co'
            });

            var generarXml = false;

            if (isSubsidiariaFeature) {
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

                log.debug("fileName", fileName);

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
                log.debug('fileUrl', fileUrl);

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

                    if (isMultibookFeature) {
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

        }

        function ValidarCaracteres_Especiales(s) {
            var AccChars = "ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðòóôõöùúûüýÿ°–—ªº·¢∞¬÷“";
            var RegChars = "SZszYAAAAAACEEEEIIIIDOOOOOUUUUYaaaaaaceeeeiiiidooooouuuuyyo--ao.     ";
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

        function Name_File(numeroEnvio) {
            var name = '';

            name = 'Dmuisca_' + completar_cero(2, paramConcepto) + '01009' + '71' + paramPeriodo + completar_cero(8, numeroEnvio);

            return name;
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