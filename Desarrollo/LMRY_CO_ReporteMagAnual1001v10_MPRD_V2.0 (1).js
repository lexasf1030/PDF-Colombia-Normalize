/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_ReporteMagAnual1001v10_MPRD_V2.0.js      ||
||                                                              ||
||  Version Date           Author        Remarks                ||
||  2.0     Sept 22 2020  LatamReady    Use Script 2.0          ||
 \= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */

/**
 *@NApiVersion 2.0
 *@NScriptType MapReduceScript
 *@NModuleScope Public
 */
define(['N/record', 'N/runtime', 'N/file', 'N/search', 'N/encode',
        'N/format', 'N/log', 'N/config', './CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js'
    ],

    function(record, runtime, file, search, encode, format, log, config, libreria) {
        var objContext = runtime.getCurrentScript();

        var LMRY_script = "LMRY_CO_ReporteMagAnual1001v10_MPRD_V2.0.js";
        var objContext = runtime.getCurrentScript();

        var paramSubsidiary = objContext.getParameter('custscript_lmry_co_f1001_v10_mprd_subsi');
        var paramPeriod = objContext.getParameter('custscript_lmry_co_f1001_v10_mprd_period');
        var paramMultibook = objContext.getParameter('custscript_lmry_co_f1001_v10_mprd_multi');
        var paramReportId = objContext.getParameter('custscript_lmry_co_f1001_v10_mprd_rptid');
        var paramReportVersionId = objContext.getParameter('custscript_lmry_co_f1001_v10_mprd_rptvid');
        var paramLogId = objContext.getParameter('custscript_lmry_co_f1001_v10_mprd_logid');
        var paramConcept = objContext.getParameter('custscript_lmry_co_f1001_v10_mprd_concep');
        var paramWithholdingFileId = objContext.getParameter('custscript_lmry_co_f1001_v10_mprd_whsid');


        var hasSubsidiariesFeature = runtime.isFeatureInEffect({
            feature: 'SUBSIDIARIES'
        });

        var hasMultibookFeature = runtime.isFeatureInEffect({
            feature: 'MULTIBOOK'
        });

        var hasJobsFeature = runtime.isFeatureInEffect({
            feature: 'JOBS'
        });

        var hasAdvancedJobsFeature = runtime.isFeatureInEffect({
            feature: 'ADVANCEDJOBS'
        });

        var language = runtime.getCurrentScript().getParameter("LANGUAGE").substring(0, 2);

        function getInputData() {

            try {
                log.error('Parametros', paramSubsidiary + '|' + paramPeriod + '|' + paramMultibook + '|' + paramReportId + '|' + paramReportVersionId + '|' + paramLogId + '|' + paramConcept + '|' + paramWithholdingFileId);

                var transactionsArray = getTransactions();

                log.error('transactionsArray', transactionsArray);

                return transactionsArray;
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


                var objResult = JSON.parse(context.value);

                if (objResult["isError"] == "T") {
                    context.write({
                        key: context.key,
                        value: objResult
                    });
                } else {
                    var accountDetailJson = getTransactionAccountDetail(objResult);

                    for (var key in accountDetailJson) {
                        context.write({
                            key: key,
                            value: accountDetailJson[key]
                        });
                    }

                }
            } catch (error) {
                log.error("Error, Objresult", objResult);
                log.error("Error, context.key", context.key);
                log.error("Error, context.key", error);
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

            try {
                var resultArray = context.values;
                var groupedResult = [],
                    objResult, pagoDeducible = 0,
                    pagoNoDeducible = 0;

                for (var i = 0; i < resultArray.length; i++) {
                    objResult = JSON.parse(resultArray[i]);

                    if (objResult["isError"] == "T") {
                        context.write({
                            key: context.key,
                            value: objResult
                        });
                        return;
                    }

                    pagoDeducible = round(pagoDeducible + Number(objResult[12]));
                    pagoNoDeducible = round(pagoNoDeducible + Number(objResult[13]));
                    groupedResult = objResult;
                }

                groupedResult[12] = pagoDeducible;
                groupedResult[13] = pagoNoDeducible;

                //log.error("groupedResult", groupedResult);

                context.write({
                    key: context.key,
                    value: groupedResult
                });

            } catch (error) {
                log.error("error reduce", error);
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
            try {
                var withholdingsJson = getWithholdingsFile();
                log.debug('json retenciones', withholdingsJson);
                var errors = [];
                var countriesJson = getCountries();
                log.debug('countriesJson',countriesJson);
                var groupingOfResults = [
                    []
                ];

                var indexGroupingOfResult = 0;
                var resultsCounter = 0;
                var minimumAmountJson = {};

                var type = context.toString();
                log.audit(type + ' Usage Consumed', context.usage);
                log.audit(type + ' Concurrency Number ', context.concurrency);
                log.audit(type + ' Number of Yields', context.yields);
                var index = '';
                context.output.iterator().each(function(key, value) {
                    var objResult = JSON.parse(value);
                    log.debug('objResult del summarize', objResult)
                    if (objResult["isError"] == "T") {
                        errors.push(JSON.stringify(objResult["error"]));
                    } else {
                        index = key;
                        //index = objResult[0] + '|' + objResult[1] + '|' + objResult[2] + '|' + objResult[3] + '|' + objResult[4] + '|' + objResult[5] + '|' + objResult[6] + '|' + objResult[7] + '|' + objResult[8] + '|' + objResult[9] + '|' + objResult[10] + '|' + objResult[11];

                        if (withholdingsJson[index] !== undefined) {

                            objResult[14] = 0;
                            objResult[15] = 0;
                            objResult[16] = Number(withholdingsJson[index][0]);
                            objResult[17] = Number(withholdingsJson[index][1]);
                            objResult[18] = Number(withholdingsJson[index][2]);
                            objResult[19] = Number(withholdingsJson[index][3]);

                        } else {
                            objResult[14] = 0;
                            objResult[15] = 0;
                            objResult[16] = 0;
                            objResult[17] = 0;
                            objResult[18] = 0;
                            objResult[19] = 0;
                        }

                        objResult[11] = countriesJson[objResult[11]] || '';

                        objResult[12] = Number(objResult[12]);
                        objResult[13] = Number(objResult[13]);
                        
                        var amountCuantiaMenor = getAmountCuantiaMenor();
                        //Cuantia Menor consideracion - ppt
                        if (Math.abs(Number(objResult[12])) < amountCuantiaMenor && Math.abs(Number(objResult[12])) != 0) {//cambiar
                            log.debug('entro al if cuantia menor',objResult[12]) 
                            if (minimumAmountJson[objResult[0]] === undefined) {
                                log.debug('entro al if undefined',objResult) 
                                minimumAmountJson[objResult[0]] = objResult;
                            } else {
                                minimumAmountJson[objResult[0]][12] = round(minimumAmountJson[objResult[0]][12] + Number(objResult[12]));
                                minimumAmountJson[objResult[0]][13] = round(minimumAmountJson[objResult[0]][13] + Number(objResult[13]));
                                minimumAmountJson[objResult[0]][16] = round(minimumAmountJson[objResult[0]][16] + Number(objResult[16]));
                                minimumAmountJson[objResult[0]][17] = round(minimumAmountJson[objResult[0]][17] + Number(objResult[17]));
                                minimumAmountJson[objResult[0]][18] = round(minimumAmountJson[objResult[0]][18] + Number(objResult[18]));
                                minimumAmountJson[objResult[0]][19] = round(minimumAmountJson[objResult[0]][19] + Number(objResult[19]));
                                
                                log.debug('minimumAmountJson elsee',minimumAmountJson)
                            }
                        } else {

                            if (showLine(objResult)) {
                                resultsCounter++;
                                if (resultsCounter == 1000) {
                                    resultsCounter = 0;
                                    indexGroupingOfResult++;
                                    groupingOfResults[indexGroupingOfResult] = []; //[[]]
                                }
                                groupingOfResults[indexGroupingOfResult].push(objResult); //[[objectresult,or,......],[......1000]]
                            }
                        }
                    }
                    return true;
                });
                log.debug("groupingOfResults", groupingOfResults);
                log.debug("minimumAmountJson", minimumAmountJson);

                if (errors.length > 0) {
                    log.debug("error", errors);
                    //libreria.sendemailTranslate(errors[0], LMRY_script, language);
                } else {
                    var dataSubsidiary = getSusbisidiaryRecordById(paramSubsidiary);
                    var dataCountrySubsidiary = dataSubsidiary[3];
                    var cuantiaCountry = dataCountrySubsidiary[0].value;
                    var auxArray = [];
                    for (var concept in minimumAmountJson) {
                        auxArray = [];

                        auxArray[0] = concept;
                        auxArray[1] = '43';
                        auxArray[2] = '222222222';
                        auxArray[3] = '';
                        auxArray[4] = '';
                        auxArray[5] = '';
                        auxArray[6] = '';
                        auxArray[7] = 'CUANTÍAS MENORES';
                        auxArray[8] = dataSubsidiary[0];//direccion
                        auxArray[9] = dataSubsidiary[1];//province
                        auxArray[10] = dataSubsidiary[2];//municipality
                        auxArray[11] = countriesJson[cuantiaCountry] || '';//country
                        auxArray[12] = Number(minimumAmountJson[concept][12]);
                        auxArray[13] = Number(minimumAmountJson[concept][13]);
                        auxArray[14] = 0;
                        auxArray[15] = 0;
                        auxArray[16] = Number(minimumAmountJson[concept][16]);
                        auxArray[17] = Number(minimumAmountJson[concept][17]);
                        auxArray[18] = Number(minimumAmountJson[concept][18]);
                        auxArray[19] = Number(minimumAmountJson[concept][19]);

                        if (showLine(auxArray)) {

                            resultsCounter++;
                            if (resultsCounter == 1000) {
                                resultsCounter = 0;
                                indexGroupingOfResult++;
                                groupingOfResults[indexGroupingOfResult] = [];
                            }
                            groupingOfResults[indexGroupingOfResult].push(auxArray);
                        }

                    }

                    if (groupingOfResults[0].length != 0) {

                        // Generar Reporte cada 00 registros
                        var shippingNumber = 0;

                        for (var i = 0; i < groupingOfResults.length; i++) {

                            shippingNumber = getShippingNumber();

                            generateXml(groupingOfResults[i], shippingNumber);

                            generateExcel(groupingOfResults[i], shippingNumber);
                        }
                    } else {
                        noData();
                    }
                    log.debug("termino summarize");
                }

            } catch (error) {
                log.error("error", error);
                //libreria.sendemailTranslate(error, LMRY_script, language);
            }
        }

        function getAmountCuantiaMenor(){
            var newAmount = search.lookupFields({
                type: 'customrecord_lmry_co_rpt_feature_version',
                id: paramReportVersionId,
                columns: ['custrecord_lmry_co_amount']
            });

            var cuantiaMenor = newAmount.custrecord_lmry_co_amount;

            return cuantiaMenor;
            
        }

        function getCountries() {

            var intDMinReg = 0;
            var intDMaxReg = 1000;
            var DbolStop = false;

            var savedSearch = search.create({
                type: 'customrecord_lmry_mx_country',
                filters: [
                    ["isinactive", "is", "F"],
                    "AND", ["custrecord_country_localization", "anyof", "48"]
                ],
                columns: [
                    'name',
                    'custrecord_lmry__mx_contrycode'
                ]
            });

            var searchResult = savedSearch.run();
            var objResult, columns, countriesJson = {};

            while (!DbolStop) {
                objResult = searchResult.getRange(intDMinReg, intDMaxReg);

                if (objResult != null) {
                    if (objResult.length != 1000) {
                        DbolStop = true;
                    }

                    for (var i = 0; i < objResult.length; i++) {
                        columns = objResult[i].columns;
                        countriesJson[objResult[i].getValue(columns[0])] = objResult[i].getValue(columns[1]);
                    }

                    intDMinReg = intDMaxReg;
                    intDMaxReg = intDMaxReg + 1000;
                }
            }
            return countriesJson;
        }

        function getWithholdingsFile() {

            var withholdingsJson = {};
            if (paramWithholdingFileId) {
                var filesIdArray = paramWithholdingFileId.split('|');
                var auxArray = [];
                var archivo;

                for (var i = 0; i < filesIdArray.length; i++) {
                    if (filesIdArray[i]) {
                        archivo = file.load({
                            id: filesIdArray[i]
                        });
                        auxArray.push(archivo.getContents());
                    }
                }

                var contenidoArray = [],
                    resultArray = [],
                    key = '';
                for (var i = 0; i < auxArray.length; i++) {
                    contenidoArray = auxArray[i].split('\r\n');
                    for (var j = 0; j < contenidoArray.length; j++) {
                        if (contenidoArray[j].length != 0) {
                            resultArray = contenidoArray[j].split('|');
                            key = resultArray[0] + '|' + resultArray[1];
                            //log.error("resultArray", resultArray);
                            withholdingsJson[key] = [resultArray[2], resultArray[3], resultArray[4], resultArray[5]];
                        }
                    }
                }

            }
            //log.error("withholdingsJson", withholdingsJson);
            return withholdingsJson;
        }

        function getTransactionAccountDetail(contextValue) {

            //log.error('context', contextValue);
            //desde aqui comenzar
            if (contextValue[0] == 'Journal') {
                var newSearch = search.create({
                    type: 'account',
                    filters: [
                        ['internalid', 'anyof', contextValue[5]],
                        'AND', ["formulatext: {custrecord_lmry_co_puc_concept.custrecord_lmry_co_puc_format_c.id}", "is", "1"],
                        "AND", ["custrecord_lmry_co_puc_id", "noneof", "@NONE@"],
                        "AND", ["type", "noneof", "FixedAsset"]
                    ],
                    columns: [
                        search.createColumn({
                            name: "formulatext",
                            formula: "{custrecord_lmry_co_puc_concept.custrecord_lmry_co_puc_number_c}",
                            label: "0. Format Number"
                        }),
                        search.createColumn({
                            name: "custrecord_lmry_co_fields_format_c",
                            join: "custrecord_lmry_co_puc_concept"
                        })
                    ]
                });
            } else {
                var newSearch = search.create({
                    type: 'account',
                    filters: [
                        ['internalid', 'anyof', contextValue[5]],
                        'AND', ["formulatext: {custrecord_lmry_co_puc_concept.custrecord_lmry_co_puc_format_c.id}", "is", "1"],
                        "AND", ["custrecord_lmry_co_puc_id", "noneof", "@NONE@"]
                    ],
                    columns: [
                        search.createColumn({
                            name: "formulatext",
                            formula: "{custrecord_lmry_co_puc_concept.custrecord_lmry_co_puc_number_c}",
                            label: "0. Format Number"
                        }),
                        search.createColumn({
                            name: "custrecord_lmry_co_fields_format_c",
                            join: "custrecord_lmry_co_puc_concept"
                        })
                    ]
                });
            }




            var objResult = newSearch.run().getRange(0, 1000);

            var resultArray = [],
                key = '';

            var entityType = '',
                entityId = ''
            var accountsDetailJson = {};

            //log.error("tam account", objResult.length);

            if (objResult && objResult.length) {
                var columns;
                for (var i = 0; i < objResult.length; i++) {
                    resultArray = [];
                    columns = objResult[i].columns;

                    // Concepto
                    resultArray[0] = objResult[i].getValue(columns[0]);

                    // entityId = contextValue[3] || contextValue[4];


                    if (contextValue[0] == 'VendBill' || contextValue[0] == 'VendCred' || contextValue[0] == 'VendPymt') {
                        entityType = 'vendor';
                        entityId = contextValue[3];
                    } else if (contextValue[0] == 'CustInvc' || contextValue[0] == 'CustCred' || contextValue[0] == 'CustPymt') {
                        entityType = 'customer';
                        entityId = contextValue[4];
                    } else if (contextValue[0] == 'ExpRept') {
                        if (contextValue[3] != '') {
                            entityType = 'vendor';
                            entityId = contextValue[3];
                        } else if (contextValue[4] != '') {
                            entityType = 'customer';
                            entityId = contextValue[4];
                        } else if (contextValue[7] != '') {
                            entityType = 'employee';
                            entityId = contextValue[7];
                        }
                    } else if (contextValue[0] == 'Check') {
                        entityType = 'Check';
                        entityId = contextValue[3];
                    } else if (contextValue[0] == 'Journal') {

                        if (contextValue[3] != '') {
                            entityType = 'vendor';
                            entityId = contextValue[3];
                        } else if (contextValue[4] != '') {
                            entityType = 'customer';
                            entityId = contextValue[4];
                        } else if (contextValue[6] != '') {
                            entityType = 'employee';
                            entityId = contextValue[6];
                        }
                    }

                    if (entityType == 'employee') {
                        var aux_arr_info = getInformationEmploy(entityType, entityId);
                    } else {
                        var aux_arr_info = getInformation(entityType, entityId);
                    }

                    if (aux_arr_info != 0) {

                        resultArray = resultArray.concat(aux_arr_info);

                        resultArray[12] = 0;
                        resultArray[13] = 0;

                        if (objResult[i].getValue(columns[1]).split(',').indexOf('1') >= 0) {
                            resultArray[12] = contextValue[1];
                        }
                        if (objResult[i].getValue(columns[1]).split(',').indexOf('2') >= 0) {
                            resultArray[13] = contextValue[2];
                        }

                        //key = resultArray[0] + '|' + resultArray[1] + '|' + resultArray[2] + '|' + resultArray[3] + '|' + resultArray[4] + '|' + resultArray[5] + '|' + resultArray[6] + '|' + resultArray[7] + '|' + resultArray[8] + '|' + resultArray[9] + '|' + resultArray[10] + '|' + resultArray[11];
                        key = resultArray[0] + '|' + entityId;
                        accountsDetailJson[key] = resultArray;

                    }

                }
            }

            return accountsDetailJson;
        }

        function getTransactions() {

            var periodStartDate = format.format({
                type: format.Type.DATE,
                value: new Date(paramPeriod, 0, 1)
            });

            var periodEndDate = format.format({
                type: format.Type.DATE,
                value: new Date(paramPeriod, 11, 31)
            });

            var savedSearch = search.load({
                id: 'customsearch_lmry_co_form_1001_pay_v10'
            });

            if (paramPeriod) {
                var startDateFilter = search.createFilter({
                    name: 'trandate',
                    operator: search.Operator.ONORAFTER,
                    values: [periodStartDate]
                });
                savedSearch.filters.push(startDateFilter);

                var endDateFilter = search.createFilter({
                    name: 'trandate',
                    operator: search.Operator.ONORBEFORE,
                    values: [periodEndDate]
                });
                savedSearch.filters.push(endDateFilter);
            }

            if (hasSubsidiariesFeature) {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: paramSubsidiary
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
                log.error("customermain");
                var customerColumn = search.createColumn({
                    name: 'formulanumeric',
                    formula: 'NVL({customermain.internalid},{customer.internalid})',
                    summary: 'GROUP'
                });
                savedSearch.columns.push(customerColumn);
            } else if ((!hasJobsFeature && !hasAdvancedJobsFeature) || (!hasJobsFeature && hasAdvancedJobsFeature) || (hasJobsFeature && hasAdvancedJobsFeature)) {
                log.error("customer");
                var customerColumn = search.createColumn({
                    name: "formulanumeric",
                    formula: "CASE WHEN NVL({job.internalid},-1) = -1 THEN {customer.internalid} ELSE {job.customer.id} end",
                    summary: "GROUP"
                });
                savedSearch.columns.push(customerColumn);
            }

            if (hasMultibookFeature) {
                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [paramMultibook]
                });
                savedSearch.filters.push(multibookFilter);

                var accountsArray = getForm1001Accounts();
                log.error("accountsArray", accountsArray);
                var accountsFilters = search.createFilter({
                    name: 'account',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: accountsArray
                });
                savedSearch.filters.push(accountsFilters);


                var accountIdColumn = search.createColumn({
                    name: 'formulanumeric',
                    formula: '{accountingtransaction.account.id}',
                    summary: 'GROUP'
                });
                savedSearch.columns.push(accountIdColumn);

                var amountFilter = search.createFilter({
                    name: 'formulanumeric',
                    operator: search.Operator.NOTEQUALTO,
                    formula: 'NVL({accountingtransaction.debitamount},0) - NVL({accountingtransaction.creditamount},0)',
                    values: '0'
                });
                savedSearch.filters.push(amountFilter);


                var pagoDeducibleColumn = search.createColumn({
                    name: 'formulacurrency',
                    formula: "CASE WHEN {taxitem} = 'SNoc-CO' OR {taxitem} = 'INoc-CO' OR NVL({custbody_lmry_type_concept.internalid},1) = 27 THEN 0 ELSE  NVL({accountingtransaction.debitamount},0) - NVL({accountingtransaction.creditamount}, 0) END",
                    summary: 'SUM'
                });
                savedSearch.columns.splice(1, 1, pagoDeducibleColumn);

                var pagoNoDeducibleColumn = search.createColumn({
                    name: 'formulacurrency',
                    formula: "CASE WHEN {taxitem} = 'SNoc-CO' OR {taxitem} = 'INoc-CO' OR NVL({custbody_lmry_type_concept.internalid},1) = 27 THEN NVL({accountingtransaction.debitamount},0) - NVL({accountingtransaction.creditamount}, 0) ELSE 0 END",
                    summary: 'SUM',
                });
                savedSearch.columns.splice(2, 1, pagoNoDeducibleColumn);

            } else {

                var accountsArray = getForm1001Accounts();
                log.error("accountsArray", accountsArray);
                var accountsFilters = search.createFilter({
                    name: 'account',
                    operator: search.Operator.IS,
                    values: accountsArray
                });
                savedSearch.filters.push(accountsFilters);

                var accountIdColumn = search.createColumn({
                    name: 'formulanumeric',
                    formula: '{account.internalid}',
                    summary: 'GROUP'
                });
                savedSearch.columns.push(accountIdColumn);
            }

            var EmployeeColumn = search.createColumn({
                name: 'formulanumeric',
                formula: "{entity.id}",
                summary: 'GROUP'
            });
            savedSearch.columns.push(EmployeeColumn);

            var MainLineColumn = search.createColumn({
                name: 'formulanumeric',
                formula: '{mainname.id}',
                summary: 'GROUP'
            });
            savedSearch.columns.push(MainLineColumn);

            var transactionsArray = [];
            if (accountsArray.length != 0) {
                var pagedData = savedSearch.runPaged({
                    pageSize: 1000
                });

                var page, auxArray;

                pagedData.pageRanges.forEach(function(pageRange) {
                    page = pagedData.fetch({
                        index: pageRange.index
                    });
                    page.data.forEach(function(result) {
                        auxArray = [];


                        // 0. Tipo de Transaccion
                        if (result.getValue(result.columns[0]) != '- None -') {
                            auxArray[0] = result.getValue(result.columns[0]);
                        } else {
                            auxArray[0] = '';
                        }

                        // 1. Pago Deducible
                        if (result.getValue(result.columns[1]) != '- None -') {
                            auxArray[1] = result.getValue(result.columns[1]);
                        } else {
                            auxArray[1] = '';
                        }

                        // 2. Pago no Deducible
                        if (result.getValue(result.columns[2]) != '- None -') {
                            auxArray[2] = result.getValue(result.columns[2]);
                        } else {
                            auxArray[2] = '';
                        }

                        // 3. Vendor
                        if (result.getValue(result.columns[3]) != '- None -') {
                            auxArray[3] = result.getValue(result.columns[3]);
                        } else {
                            auxArray[3] = '';
                        }

                        // 4. Customer
                        if (result.getValue(result.columns[4]) != '- None -') {
                            auxArray[4] = result.getValue(result.columns[4]);
                        } else {
                            auxArray[4] = '';
                        }

                        // 5. Account
                        if (result.getValue(result.columns[5]) != '- None -') {
                            auxArray[5] = result.getValue(result.columns[5]);
                        } else {
                            auxArray[5] = '';
                        }

                        // 6. Employee
                        if (result.getValue(result.columns[6]) != '- None -') {
                            auxArray[6] = result.getValue(result.columns[6]);
                        } else {
                            auxArray[6] = '';
                        }

                        // 7. Main Line
                        if (result.getValue(result.columns[7]) != '- None -') {
                            auxArray[7] = result.getValue(result.columns[7]);
                        } else {
                            auxArray[7] = '';
                        }

                        transactionsArray.push(auxArray);
                    });
                });
            }


            return transactionsArray;
        }

        function getForm1001Accounts() {
            var accountsArray = [];
            var newSearch = search.create({
                type: 'account',
                filters: [
                    ["isinactive", "is", "F"],
                    'AND', ["formulatext: {custrecord_lmry_co_puc_concept.custrecord_lmry_co_puc_format_c.id}", "is", "1"]
                ],
                columns: ["internalid"]
            });

            var objResult = newSearch.run().getRange(0, 1000);

            if (objResult && objResult.length) {

                for (var i = 0; i < objResult.length; i++) {
                    accountsArray.push(objResult[i].getValue("internalid"));
                }
            }

            return accountsArray;
        }

        function getAccountConcept(accountId) {

            var newSearch = search.create({
                type: 'account',
                filters: [
                    ['internalid', 'anyof', accounts],
                    'AND', ["formulatext: {custrecord_lmry_co_puc_concept.custrecord_lmry_co_puc_format_c.id}", "is", "1"]
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

            var accountRecord = search.lookupFields({
                type: 'account',
                id: accountId,
                columns: ['custrecord_lmry_co_puc_concept']
            });

            if (accountRecord.custrecord_lmry_co_puc_concept.length) {
                return accountRecord.custrecord_lmry_co_puc_concept[0].text.substring(0, 4);
            } else {
                return '';
            }
        }

        function getInformation(entityType, entityId) {

            var auxArray = [];

            if (entityType == 'Check') {
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
                        search.createColumn({ name: "internalid", label: "Internal ID" })
                    ]
                });
                var objResult = entitySearchObj.run().getRange(0, 10);
                var columns = objResult[0].columns;
                // 0. Tipo de entidad
                if (objResult[0].getValue(columns[0]) == 'Vendor') {
                    var entidad = 'vendor';
                } else {
                    var entidad = 'customer';
                }
                //log.error("entidad", entidad);
                //log.error("entityId", entityId);

            } else {
                var entidad = entityType;
            }


            if (entidad != '' && entityId != '') {

                var newSearch = search.create({
                    type: entidad,
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
                            formula: "{address1}",
                            label: "5. Dirección"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "{custentity_lmry_municcode}",
                            label: "6. Departamento y Municipio"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "{custentity_lmry_country}",
                            label: "7. Pais"
                        })
                    ]
                });

                var objResult = newSearch.run().getRange(0, 1000);
                if (objResult && objResult.length) {
                    var columns = objResult[0].columns;

                    // 0. Tipo de Documento
                    auxArray[0] = objResult[0].getValue(columns[0]);

                    // 1. NIT
                    auxArray[1] = cleanNit(objResult[0].getValue(columns[1]));

                    // 2. Apellido Paterno
                    if (objResult[0].getValue(columns[2]).split(' ')[0]) {
                        auxArray[2] = objResult[0].getValue(columns[2]).split(' ')[0];
                    } else {
                        auxArray[2] = '';
                    }

                    // 3. Apellido Materno
                    if (objResult[0].getValue(columns[2]).split(' ')[1]) {
                        auxArray[3] = objResult[0].getValue(columns[2]).split(' ')[1];
                    } else {
                        auxArray[3] = '';
                    }


                    // 4. Primer Nombre
                    if (objResult[0].getValue(columns[3]).split(' ')[0]) {
                        auxArray[4] = objResult[0].getValue(columns[3]).split(' ')[0];
                    } else {
                        auxArray[4] = '';
                    }

                    // 5. Segundo Nombre
                    if (objResult[0].getValue(columns[3]).split(' ')[1]) {
                        auxArray[5] = objResult[0].getValue(columns[3]).split(' ')[1];
                    } else {
                        auxArray[5] = '';
                    }

                    // 6. Razón Social
                    auxArray[6] = objResult[0].getValue(columns[4]);

                    // 7. Dirección
                    auxArray[7] = objResult[0].getValue(columns[5]);

                    if (objResult[0].getValue(columns[6])) {
                        // 8. Código de Departamento
                        auxArray[8] = objResult[0].getValue(columns[6]).substring(0, 2);

                        // 9. Municipio
                        auxArray[9] = objResult[0].getValue(columns[6]).substring(2, 5);
                    } else {
                        // 8. Código de Departamento
                        auxArray[8] = '';

                        // 9. Municipio
                        auxArray[9] = '';
                    }

                    // 10. País
                    auxArray[10] = objResult[0].getValue(columns[7]);
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
                            formula: "{lastname}",
                            label: "2. Apellidos"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "{firstname}",
                            label: "3. Nombres"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "' '",
                            label: "4. Razón Social"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "{address.address1}",
                            label: "5. Dirección"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "{address.custrecord_lmry_addr_prov_id}",
                            label: "6. LATAM - PROVINCE ID"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            //formula: "{custentity_lmry_municcode}",
                            formula: "{address.custrecord_lmry_addr_city_id}",
                            label: "7. LATAM - CITY ID"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "{address.country}",
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
                    auxArray[1] = cleanNit(objResult[0].getValue(columns[1]));

                    // 2. Apellido Paterno
                    if (objResult[0].getValue(columns[2]).split(' ')[0]) {
                        auxArray[2] = objResult[0].getValue(columns[2]).split(' ')[0];
                    } else {
                        auxArray[2] = '';
                    }

                    // 3. Apellido Materno
                    if (objResult[0].getValue(columns[2]).split(' ')[1]) {
                        auxArray[3] = objResult[0].getValue(columns[2]).split(' ')[1];
                    } else {
                        auxArray[3] = '';
                    }


                    // 4. Primer Nombre
                    if (objResult[0].getValue(columns[3]).split(' ')[0]) {
                        auxArray[4] = objResult[0].getValue(columns[3]).split(' ')[0];
                    } else {
                        auxArray[4] = '';
                    }

                    // 5. Segundo Nombre
                    if (objResult[0].getValue(columns[3]).split(' ')[1]) {
                        auxArray[5] = objResult[0].getValue(columns[3]).split(' ')[1];
                    } else {
                        auxArray[5] = '';
                    }

                    // 6. Razón Social
                    auxArray[6] = objResult[0].getValue(columns[4]);

                    // 7. Dirección
                    auxArray[7] = objResult[0].getValue(columns[5]);
   
                    // 8. Código de Departamento
                    auxArray[8] = objResult[0].getValue(columns[6]);
                    
                    // 9. Municipio
                    auxArray[9] = objResult[0].getValue(columns[7]).substr(-3);
                    
                    // 10. País
                    auxArray[10] = objResult[0].getValue(columns[8]);
                }
            }
            return auxArray;
        }

        function getNameFile(shippingNumber) {
            return 'Dmuisca_' + completeZero(2, paramConcept) + '01001' + '10' + paramPeriod + completeZero(8, shippingNumber);
        }

        function saveFile(fileName, fileContent, extension, isFirstPrint) {
            var folderId = objContext.getParameter({
                name: 'custscript_lmry_file_cabinet_rg_co'
            });

            if (folderId != '' && folderId != null) {
                if (extension == '.xls') {
                    var f1001File = file.create({
                        name: fileName,
                        fileType: file.Type.EXCEL,
                        contents: fileContent,
                        folder: folderId
                    });

                } else {
                    var f1001File = file.create({
                        name: fileName,
                        fileType: file.Type.XMLDOC,
                        contents: fileContent,
                        encoding: file.Encoding.UTF8,
                        folder: folderId
                    });
                }

                var fileId = f1001File.save();

                f1001File = file.load({
                    id: fileId
                });

                var getURL = objContext.getParameter({
                    name: 'custscript_lmry_netsuite_location'
                });

                var fileUrl = '';

                if (getURL != '') {
                    fileUrl += 'https://' + getURL;
                }

                fileUrl += f1001File.url;

                log.error("fileUrl - " + extension, fileUrl)

                if (fileId) {
                    var usuario = runtime.getCurrentUser();
                    var employee = search.lookupFields({
                        type: search.Type.EMPLOYEE,
                        id: usuario.id,
                        columns: ['firstname', 'lastname']
                    });
                    var usuarioName = employee.firstname + ' ' + employee.lastname;

                    if (paramReportId != null) {
                        var reportName = search.lookupFields({
                            type: 'customrecord_lmry_co_features',
                            id: paramReportId,
                            columns: ['name']
                        }).name;
                    } else {
                        var reportName = ''
                    }

                    if (hasSubsidiariesFeature) {
                        var companyName = search.lookupFields({
                            type: search.Type.SUBSIDIARY,
                            id: paramSubsidiary,
                            columns: ['legalname']
                        }).legalname;
                    } else {
                        var pageConfig = config.load({
                            type: config.Type.COMPANY_INFORMATION
                        });
                        var companyName = pageConfig.getValue('legalname');
                    }

                    log.error("isFirstPrint", isFirstPrint);

                    if (isFirstPrint) {
                        log.error("entro a load");
                        var logRecord = record.load({
                            type: 'customrecord_lmry_co_rpt_generator_log',
                            id: paramLogId
                        });
                    } else {
                        log.error("entro a create");
                        var logRecord = record.create({
                            type: 'customrecord_lmry_co_rpt_generator_log'
                        });
                    }

                    //Nombre de Archivo
                    logRecord.setValue({
                        fieldId: 'custrecord_lmry_co_rg_name',
                        value: fileName
                    });

                    //Url de Archivo
                    logRecord.setValue({
                        fieldId: 'custrecord_lmry_co_rg_url_file',
                        value: fileUrl
                    });

                    //Nombre de Reporte
                    logRecord.setValue({
                        fieldId: 'custrecord_lmry_co_rg_transaction',
                        value: reportName
                    });

                    //Nombre de Subsidiaria
                    logRecord.setValue({
                        fieldId: 'custrecord_lmry_co_rg_subsidiary',
                        value: companyName
                    });

                    //Periodo
                    logRecord.setValue({
                        fieldId: 'custrecord_lmry_co_rg_postingperiod',
                        value: paramPeriod
                    });

                    if (hasMultibookFeature) {
                        var multibookName = search.lookupFields({
                            type: search.Type.ACCOUNTING_BOOK,
                            id: paramMultibook,
                            columns: ['name']
                        }).name;

                        //Multibook
                        logRecord.setValue({
                            fieldId: 'custrecord_lmry_co_rg_multibook',
                            value: multibookName
                        });
                    }

                    //Creado Por
                    logRecord.setValue({
                        fieldId: 'custrecord_lmry_co_rg_employee',
                        value: usuarioName
                    });

                    logRecord.save();
                    libreria.sendrptuser(reportName, 3, fileName);
                }
            } else {
                log.error({
                    title: 'Creacion de File:',
                    details: 'No existe el folder'
                });
            }
        }

        function generateExcel(objResult, shippingNumber) {
            var xlsString = '';

            if (hasSubsidiariesFeature) {
                var subsidiaryRecord = search.lookupFields({
                    type: search.Type.SUBSIDIARY,
                    id: paramSubsidiary,
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

            if (hasMultibookFeature) {
                var multibookName = search.lookupFields({
                    type: search.Type.ACCOUNTING_BOOK,
                    id: paramMultibook,
                    columns: ['name']
                }).name;
            }
            /*
            periodStartDate format.format({
                value: new Date(paramPeriod, 0, 1),
                type: format.Type.DATE
            });

            var periodEndDate = format.format({
                value: new Date(paramPeriod, 11, 31),
                type: format.Type.DATE
            });
            */
            var periodStartDate = "01/01/" + paramPeriod;

            var periodEndDate = "31/12/" + paramPeriod;

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

            var language = runtime.getCurrentScript().getParameter("LANGUAGE").substring(0,2);
            if(language == 'en'){
                var cabecera = ["FORM 1001: PAYMENTS OR CREDITS ON ACCOUNT AND WITHHOLDINGS MADE","Company Name",
            "Tax Number","Period","Multibook","Concept","Type of Document","NID","1st Last Name","2nd Last Name",
            "1st Name","2nd Name","Company Name","Address","Department","Municipality","Country","Payment or credit to deductible account",
            "Payment or credit to NOT deductible account","VAT greater value of the cost or deductible expense",
            "VAT greater value of the cost or non-deductible expense","Ret. Source Pract. Income","Ret. Source Assumed Income",
            "Ret. Source Pract. VAT","Ret. Source Pract. Non-domiciled VAT", "Origin", "Date", "Time"];
            } else if (language == 'pt'){
                var cabecera = ["FORMULARIO 1001: PAGAMENTOS OU CREDITOS POR CONTA E DEDUÇÕES EFECTUADAS","Razão Social",
            "Numero de identificação fiscal","Periodo","Multibook","Conceito","Tipo de Documento","NID","1er Apelli","2do Apelli",
            "1er Nombre","2do Nombre","Razão Social","Endereço","Departamento","Municipio","Pais","Pagamento dedutivel ou credito em conta","Pagamento ou credito por conta NÃO dedutível"
            ,"IVA valor mais elevado do custo ou da despesa dedutivel","IVA valor mais elevado do custo ou despesa não dedutivel","Ret. Fonte Pract. Rendimento","Ret. Fonte Asumida Rendimento",
            "Ret. Fonte Pract. Iva Reg. Comum","Ret. Fonte Pract. Iva não domiciliado", "Origem", "Data", "Hora"];
            } else {
            var cabecera = ["FORMULARIO 1001: PAGOS O ABONOS EN CUENTA Y RETENCIONES PRACTICADAS","Razon Social",
            "Tax Number","Periodo","Multibook","Concepto","Tipo de Documento","NID","1er Apelli","2do Apelli",
            "1er Nombre","2do Nombre","Razon Social","Direccion","Departamento","Municipio","Pais","Pago o abono en cuenta deducible","Pago o abono en cuenta NO deducible"
            ,"Iva mayor valor del costo o gasto deducible","Iva mayor valor del costo o gasto no deducible","Ret. Fuente Pract. Renta","Ret. Fuente Asumida Renta",
            "Ret. Fuente Pract. Iva Reg. Comun","Ret. Fuente Pract. Iva No Domiciliados", "Origen", "Fecha", "Hora"];
            }

            //Cabecera
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell ss:StyleID="s21"><Data ss:Type="String"> '+cabecera[0] + ' </Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row></Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell><Data ss:Type="String">'+ cabecera[1] +': ' + validarAcentos(companyName) + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell><Data ss:Type="String">'+ cabecera[2] +': ' + companyRuc + '</Data></Cell>';
            xlsString += '</Row>';
            xlsString += '<Row>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell></Cell>';
            xlsString += '<Cell><Data ss:Type="String">'+ cabecera[3] +': ' + periodStartDate + ' al ' + periodEndDate + '</Data></Cell>';
            xlsString += '</Row>';
            if (hasMultibookFeature) {
                xlsString += '<Row>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell></Cell>';
                xlsString += '<Cell><Data ss:Type="String">'+ cabecera[4] +': ' + multibookName + '</Data></Cell>';
                xlsString += '</Row>';
            }
            // PDF Normalized
			xlsString += '<Row>';
			xlsString += '<Cell></Cell>';
			xlsString += '<Cell></Cell>';
			xlsString += '<Cell></Cell>';
			xlsString += '<Cell><Data ss:Type="String">' + cabecera[25] + ": Netsuite"+ '</Data></Cell>';
			xlsString += '</Row>';

			xlsString += '<Row>';
			xlsString += '<Cell></Cell>';
			xlsString += '<Cell></Cell>';
			xlsString += '<Cell></Cell>';
			xlsString += '<Cell><Data ss:Type="String">' + cabecera[26] + ": "+ todays+'</Data></Cell>';
			xlsString += '</Row>';
			
			xlsString += '<Row>';
			xlsString += '<Cell></Cell>';
			xlsString += '<Cell></Cell>';
			xlsString += '<Cell></Cell>';
			xlsString += '<Cell><Data ss:Type="String">' + cabecera[27] + ": "+ currentTime+'</Data></Cell>';
			xlsString += '</Row>';
			// End PDF Normalized 

            xlsString += '<Row></Row>';
            xlsString += '<Row></Row>';
            xlsString += '<Row>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> '+ cabecera[5] +' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> '+ cabecera[6] +' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> '+ cabecera[7] +' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> '+ cabecera[8] +' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> '+ cabecera[9] +' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> '+ cabecera[10] +' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> '+ cabecera[11] +' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> '+ cabecera[12] +' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> '+ cabecera[13] +' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> '+ cabecera[14] +' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> '+ cabecera[15] +' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> '+ cabecera[16] +' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> '+ cabecera[17] +' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> '+ cabecera[18] +' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> '+ cabecera[19] +' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> '+ cabecera[20] +'  </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> '+ cabecera[21] +' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> '+ cabecera[22] +' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> '+ cabecera[23] +' </Data></Cell>' +
                '<Cell ss:StyleID="s21"><Data ss:Type="String"> '+ cabecera[24] +' </Data></Cell>' +
                '</Row>';

            for (var i = 0; i < objResult.length; i++) {
                xlsString += '<Row>';

                //0.  Concepto
                xlsString += '<Cell><Data ss:Type="String">' + objResult[i][0] + '</Data></Cell>';

                //1.  Tipo de Documento
                xlsString += '<Cell><Data ss:Type="String">' + objResult[i][1] + '</Data></Cell>';

                //2.  NIT
                xlsString += '<Cell><Data ss:Type="String">' + objResult[i][2] + '</Data></Cell>';

                //3.  1er Apellido
                xlsString += '<Cell><Data ss:Type="String">' + validarAcentos(objResult[i][3]) + '</Data></Cell>';

                //4.  2do Apellido
                xlsString += '<Cell><Data ss:Type="String">' + validarAcentos(objResult[i][4]) + '</Data></Cell>';

                //5.  1er Nombre
                xlsString += '<Cell><Data ss:Type="String">' + validarAcentos(objResult[i][5]) + '</Data></Cell>';

                //6.  2do Nombre
                xlsString += '<Cell><Data ss:Type="String">' + validarAcentos(objResult[i][6]) + '</Data></Cell>';

                //7.  Razón Social
                xlsString += '<Cell><Data ss:Type="String">' + validarAcentos(objResult[i][7]) + '</Data></Cell>';

                //8.  Dirección
                xlsString += '<Cell><Data ss:Type="String">' + validarAcentos(objResult[i][8]) + '</Data></Cell>';

                //9.  Departamento
                xlsString += '<Cell><Data ss:Type="String">' + validarAcentos(objResult[i][9]) + '</Data></Cell>';

                //10. Municipio
                xlsString += '<Cell><Data ss:Type="String">' + objResult[i][10] + '</Data></Cell>';

                //11. País
                xlsString += '<Cell><Data ss:Type="String">' + objResult[i][11] + '</Data></Cell>';


                //12. Pago o abono en cuenta deducible
                xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + Math.abs(Number(objResult[i][12])).toFixed(0) + '</Data></Cell>';

                //13. Pago o abono en cuenta no deducible
                xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + Math.abs(Number(objResult[i][13])) + '</Data></Cell>';

                //14. IVA mayor valor del costo o gasto deducible
                xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + Math.abs(Number(objResult[i][14])) + '</Data></Cell>';

                //15. IVA mayor valor del costo o gasto no deducible
                xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + Math.abs(Number(objResult[i][15])) + '</Data></Cell>';

                //16. Retención en la fuente practicada Renta
                log.error('objResult', objResult[i]);
                if (objResult[i][16] != null && objResult[i][16] != '- None -' && objResult[i][16] != '') {
                    xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + Math.abs(Number(objResult[i][16])).toFixed(0) + '</Data></Cell>';
                } else {
                    xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + 0 + '</Data></Cell>';
                }

                //17. Retención en la fuente asumida Renta
                if (objResult[i][17] != null && objResult[i][17] != '- None -' && objResult[i][17] != '') {
                    xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + Math.abs(Number(objResult[i][17])).toFixed(0) + '</Data></Cell>';
                } else {
                    xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number"></Data></Cell>';
                }


                //18. Retención en la fuente practicada IVA Régimen común
                xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + Math.abs(Number(objResult[i][18])).toFixed(0) + '</Data></Cell>';

                //19. Retención en la fuente practicada IVA no domiciliados
                xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + Math.abs(Number(objResult[i][19])).toFixed(0) + '</Data></Cell>';


                xlsString += '</Row>';
            }
            xlsString += '</Table></Worksheet></Workbook>';

            var excelString = encode.convert({
                string: xlsString,
                inputEncoding: encode.Encoding.UTF_8,
                outputEncoding: encode.Encoding.BASE_64
            });

            var fileName = getNameFile(shippingNumber) + '.xls';
            saveFile(fileName, excelString, '.xls', false);

        }

        function generateXml(objResult, shippingNumber) {
            var xmlString = '';

            var today = getTimeZoneDate(new Date());
            var year = today.getFullYear();
            var month = completeZero(2, today.getMonth() + 1);
            var day = completeZero(2, today.getDate());
            var hour = completeZero(2, today.getHours());
            var min = completeZero(2, today.getMinutes());
            var sec = completeZero(2, today.getSeconds());
            today = year + '-' + month + '-' + day + 'T' + hour + ':' + min + ':' + sec;

            xmlString += '<?xml version="1.0" encoding="UTF-8"?> \r\n';

            xmlString += '<mas xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"> \r\n';
            xmlString += '<Cab> \r\n';
            xmlString += '<Ano>' + paramPeriod + '</Ano> \r\n';
            xmlString += '<CodCpt>' + paramConcept + '</CodCpt> \r\n';
            xmlString += '<Formato>1001</Formato> \r\n';
            xmlString += '<Version>10</Version> \r\n';
            xmlString += '<NumEnvio>' + shippingNumber + '</NumEnvio> \r\n';
            xmlString += '<FecEnvio>' + today + '</FecEnvio> \r\n';
            xmlString += '<FecInicial>' + paramPeriod + '-01-01</FecInicial> \r\n';
            xmlString += '<FecFinal>' + paramPeriod + '-12-31</FecFinal> \r\n';
            xmlString += '<ValorTotal> ' + 'totalValue' + ' </ValorTotal> \r\n';
            xmlString += '<CantReg>' + objResult.length + '</CantReg> \r\n';
            xmlString += '</Cab>\r\n';

            var totalValue = 0;
            for (var i = 0; i < objResult.length; i++) {
                totalValue += (Number(objResult[i][0]) || 0);

                xmlString += '<pagos ' +
                    'cpt = "' + objResult[i][0] + '" ' +
                    'tdoc = "' + objResult[i][1] + '" ' +
                    'nid = "' + objResult[i][2] + '" ' +
                    'apl1 = "' + validarAcentos(objResult[i][3].replace(/&/g, '&amp;').replace(/\"/g, '&quot;').replace(/ˋ/g, '&apos;') + '" ') +
                    'apl2 = "' + validarAcentos(objResult[i][4].replace(/&/g, '&amp;').replace(/\"/g, '&quot;').replace(/ˋ/g, '&apos;') + '" ') +
                    'nom1 = "' + validarAcentos(objResult[i][5].replace(/&/g, '&amp;').replace(/\"/g, '&quot;').replace(/ˋ/g, '&apos;') + '" ') +
                    'nom2 = "' + validarAcentos(objResult[i][6].replace(/&/g, '&amp;').replace(/\"/g, '&quot;').replace(/ˋ/g, '&apos;') + '" ') +
                    'raz = "' + validarAcentos(objResult[i][7].replace(/&/g, '&amp;').replace(/\"/g, '&quot;').replace(/ˋ/g, '&apos;') + '" ') +
                    'dir = "' + validarAcentos(objResult[i][8].replace(/&/g, '&amp;').replace(/\"/g, '&quot;').replace(/ˋ/g, '&apos;') + '" ') +
                    'dpto = "' + validarAcentos(objResult[i][9]) + '" ' +
                    'mun = "' + objResult[i][10] + '" ' +
                    'pais = "' + objResult[i][11] + '" ' +
                    'pago = "' + Math.abs(Number(objResult[i][12])).toFixed(0) + '" ' +
                    'pnded = "' + Math.abs(Number(objResult[i][13])).toFixed(0) + '" ' +
                    'ided = "' + Math.abs(Number(objResult[i][14])).toFixed(0) + '" ' +
                    'inded = "' + Math.abs(Number(objResult[i][15])).toFixed(0) + '" ' +
                    'retp = "' + Math.abs(Number(objResult[i][16])).toFixed(0) + '" ' +
                    'reta = "' + Math.abs(Number(objResult[i][17])).toFixed(0) + '" ' +
                    'comun = "' + Math.abs(Number(objResult[i][18])).toFixed(0) + '" ' +
                    'ndom = "' + Math.abs(Number(objResult[i][19])).toFixed(0) + '"/>\r\n';
            }
            xmlString += '</mas> \r\n';
            xmlString = xmlString.replace('totalValue', totalValue);
            log.error("totalValue", totalValue);
            var fileName = getNameFile(shippingNumber) + '.xml';
            saveFile(fileName, xmlString, '.xml', true);
        }
        function validarAcentos(s) {
            var AccChars = "ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðñòóôõöùúûüýÿ&°–—ªº·";
            var RegChars = "SZszYAAAAAACEEEEIIIIDÑOOOOOUUUUYaaaaaaceeeeiiiidñooooouuuuyy&o--ao.";

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

        function noData() {

            var usuario = runtime.getCurrentUser();

            var employee = search.lookupFields({
                type: search.Type.EMPLOYEE,
                id: usuario.id,
                columns: ['firstname', 'lastname']
            });
            var usuarioName = employee.firstname + ' ' + employee.lastname;


            var logRecord = record.load({
                type: 'customrecord_lmry_co_rpt_generator_log',
                id: paramLogId
            });

            //Nombre de Archivo
            logRecord.setValue({
                fieldId: 'custrecord_lmry_co_rg_name',
                value: 'No existe informacion para los criterios seleccionados.'
            });

            //Creado Por
            logRecord.setValue({
                fieldId: 'custrecord_lmry_co_rg_employee',
                value: usuarioName
            });

            var recordId = logRecord.save();
        }

        function getShippingNumber() {
            var numeroLote = 1;

            var newSearch = search.create({
                type: 'customrecord_lmry_co_lote_rpt_magnetic',
                filters: [
                    search.createFilter({
                        name: 'internalid',
                        join: 'custrecord_lmry_co_id_magnetic_rpt',
                        operator: search.Operator.IS,
                        values: [paramReportVersionId]
                    }),
                    search.createFilter({
                        name: 'internalid',
                        join: 'custrecord_lmry_co_subsidiary',
                        operator: search.Operator.IS,
                        values: [paramSubsidiary]
                    }),
                    search.createFilter({
                        name: 'custrecord_lmry_co_year_issue',
                        operator: search.Operator.IS,
                        values: [paramPeriod]
                    })
                ],
                columns: ['internalid', 'custrecord_lmry_co_lote']
            });
            var objResult = newSearch.run().getRange(0, 1000);

            if (objResult == null || objResult.length == 0) {

                var loteXRptMgnRecord = record.create({
                    type: 'customrecord_lmry_co_lote_rpt_magnetic'
                });

                loteXRptMgnRecord.setValue({
                    fieldId: 'custrecord_lmry_co_id_magnetic_rpt',
                    value: paramReportVersionId
                });

                loteXRptMgnRecord.setValue({
                    fieldId: 'custrecord_lmry_co_year_issue',
                    value: paramPeriod
                });

                loteXRptMgnRecord.setValue({
                    fieldId: 'custrecord_lmry_co_lote',
                    value: numeroLote
                });

                loteXRptMgnRecord.setValue({
                    fieldId: 'custrecord_lmry_co_subsidiary',
                    value: paramSubsidiary
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

        function getSusbisidiaryRecordById(subsidiaryId) {

            if (hasSubsidiariesFeature) {
                var subsidiaryRecord = search.lookupFields({
                    type: search.Type.SUBSIDIARY,
                    id: subsidiaryId,
                    columns: ['address.address1','address.custrecord_lmry_addr_city_id','address.custrecord_lmry_addr_prov_id','address.country']
                });
            }     
            var addressSubsidiary = subsidiaryRecord['address.address1'];
            var provId = subsidiaryRecord['address.custrecord_lmry_addr_prov_id'];
            var cityId = subsidiaryRecord['address.custrecord_lmry_addr_city_id'].substr(-3);
            var countryId = subsidiaryRecord['address.country'];
            return [addressSubsidiary, provId, cityId, countryId];
        }


        function cleanNit(str) {
            str=str.replace(/,/g,"");
            str=str.replace(/-/g,"");
            str=str.replace(/\s/g,"");
            str=str.replace(/\./g,"");
            return str;
        }


        function round(number) {
            return Math.round(Number(number) * 100) / 100;
        }

        function showLine(array) {
            var showLine = false;

            if (!(Number(Math.abs(array[12]).toFixed(0)) == 0 &&
                    Number(Math.abs(array[13]).toFixed(0)) == 0 &&
                    Number(Math.abs(array[14]).toFixed(0)) == 0 &&
                    Number(Math.abs(array[15]).toFixed(0)) == 0 &&
                    Number(Math.abs(array[16]).toFixed(0)) == 0 &&
                    Number(Math.abs(array[17]).toFixed(0)) == 0 &&
                    Number(Math.abs(array[18]).toFixed(0)) == 0 &&
                    Number(Math.abs(array[19]).toFixed(0)) == 0)) {
                showLine = true;
            }

            return showLine;
        }

        function getTimeZoneDate(date) {
            var timeZone = runtime.getCurrentScript().getParameter("TIMEZONE");

            var timeZoneDateObject = date;

            if (timeZone) {
                timeZoneDateString = format.format({
                    value: date,
                    type: format.Type.DATETIME,
                    timezone: timeZone
                });

                timeZoneDateObject = format.parse({
                    value: timeZoneDateString,
                    type: format.Type.DATE,
                    timezone: timeZone
                });
            }

            return timeZoneDateObject;
        }

        function completeZero(long, valor) {
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
