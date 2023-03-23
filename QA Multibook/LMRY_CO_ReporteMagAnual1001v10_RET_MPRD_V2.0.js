/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_ReporteMagAnual1001v10_RET_MPRD_V2.0.js  ||
||                                                              ||
||  Version Date           Author        Remarks                ||
||  2.0     Sept 22 2020  LatamReady    Use Script 2.0          ||
 \= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */

/**
 *@NApiVersion 2.0
 *@NScriptType MapReduceScript
 *@NModuleScope Public
 */
 define(['N/record', 'N/task', 'N/runtime', 'N/file', 'N/search', 'N/encode',
 'N/format', 'N/log', './CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js', "/SuiteBundles/Bundle 37714/Latam_Library/LMRY_LibraryReport_LBRY_V2.js"
],

function(record, task, runtime, file, search, encode, format, log, libreria, libReport) {

 var LMRY_script = "LMRY_CO_ReporteMagAnual1001v10_RET_MPRD_V2.0.js";
 var objContext = runtime.getCurrentScript();

 var paramSubsidiary = objContext.getParameter('custscript_lmry_co_f1001_ret_v10_subsi');
 var paramPeriod = objContext.getParameter('custscript_lmry_co_f1001_ret_v10_period');
 var paramMultibook = objContext.getParameter('custscript_lmry_co_f1001_ret_v10_multi');
 var paramReportId = objContext.getParameter('custscript_lmry_co_f1001_ret_v10_rptid');
 var paramReportVersionId = objContext.getParameter('custscript_lmry_co_f1001_ret_v10_rptvid');
 var paramLogId = objContext.getParameter('custscript_lmry_co_f1001_ret_v10_logid');
 var paramConcept = objContext.getParameter('custscript_lmry_co_f1001_ret_v10_concep');


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

         log.error('Parametros', paramSubsidiary + '|' + paramPeriod + '|' + paramMultibook + '|' + paramReportId + '|' + paramReportVersionId + '|' + paramLogId + '|' + paramConcept + '|' + hasJobsFeature + '|' + hasAdvancedJobsFeature);

         var journalsManuales = getJournalsAMano();
         var creditNotesArray = getCreditNotes();
         var MaincreditNotesArray = MaingetCreditNotes();

         log.debug('journalsManuales', journalsManuales);

         log.debug('creditNotesArray', creditNotesArray.length);

         log.debug('MaincreditNotesArray', MaincreditNotesArray.length);

         var transactionArray = creditNotesArray.concat(journalsManuales);
         transactionArray = transactionArray.concat(MaincreditNotesArray);

         log.debug('transactionArray', transactionArray);

         return transactionArray;
     } catch (error) {
         log.error('FIX ME', error);
         return [{
             isError: "T",
             error: error
         }];
     }
 }

 function map(context) {

     var objResult = JSON.parse(context.value);

     if (objResult["isError"] == "T") {
         context.write({
             key: context.key,
             value: objResult
         });
     } else {

         if (objResult['type'] == 'xLineas') {

             var objResultFiltrado = getTaxResultFiltered(objResult);

             var accountDetailJson = objResultFiltrado.length > 0 ? getCreditNoteAccountsDetail(objResultFiltrado) : {};

             for (var key in accountDetailJson) {
                 context.write({
                     key: key,
                     value: accountDetailJson[key]
                 });
             }
         } else if (objResult[objResult.length - 1] == 'xTotales') {

             var accountDetailJson = MaingetCreditNoteAccountsDetail(objResult);

             for (var key in accountDetailJson) {
                 context.write({
                     key: key,
                     value: accountDetailJson[key]
                 });
             }
         } else {

             var accountDetailJson = getCreditNoteAccountsDetail(objResult);

             for (var key in accountDetailJson) {
                 context.write({
                     key: key,
                     value: accountDetailJson[key]
                 });
             }
         }

     }

 }

 // '1||||||||':[,,,,,,va1,va2,va3,va4],'2||||||||':[,,,,,,va1,va2,va3,va4],'1||||||||':[,,,,,,va3,va2y,vay3,vay4]

 function reduce(context) {
     //'1||||||||':[,,,,,,va1,va2,va3,va4],[,,,,,,va3,va2y,vay3,vay4],..]

     try {
         var resultArray = context.values;
         var groupedResult = [],
             objResult = [],
             reteIVA_1 = 0,
             reteIVA_2 = 0,
             reteFTE_1 = 0,
             reteFTE_2 = 0;

         log.error('resultArray reduce', resultArray);

         for (var i = 0; i < resultArray.length; i++) {
             objResult = JSON.parse(resultArray[i]);

             if (objResult["isError"] == "T") {
                 context.write({
                     key: context.key,
                     value: objResult
                 });
                 return;
             }
             reteFTE_1 = round(reteFTE_1 + Number(objResult[12]));
             reteFTE_2 = round(reteFTE_2 + Number(objResult[13]));
             reteIVA_1 = round(reteIVA_1 + Number(objResult[14]));
             reteIVA_2 = round(reteIVA_2 + Number(objResult[15]));
         }
         groupedResult = objResult;
         groupedResult[12] = reteFTE_1;
         groupedResult[13] = reteFTE_2;
         groupedResult[14] = reteIVA_1;
         groupedResult[15] = reteIVA_2;
         //'1||||||||':[,,,,,,Sva1,Sva2,Sva3,Sva4],'2||||||||':[,,,,,,Sva1,Sva2,Sva3,Sva4]
         context.write({
             key: context.key,
             value: groupedResult
         });

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
     log.error('LLEGO', 'summarize');
     try {
         //'1||||||||':"[,,,,,,Sva1,Sva2,Sva3,Sva4]"",'2||||||||':"[,,,,,,Sva1,Sva2,Sva3,Sva4]""
         var errors = [];

         var fileString = '',
             rowString = '',
             fileSize = 0,
             fileNumber = 0,
             filesId = "";
         context.output.iterator().each(function(key, value) {
             var objKey = key;
             var objResult = JSON.parse(value);

             //log.error('objKey', 'objKey');
             log.error('objResult', objResult);

             if (objResult["isError"] == "T") {
                 errors.push(JSON.stringify(objResult["error"]));
             } else {
                 //rowString = objKey + '|' + objResult.join('|') + '\r\n'; // "b|b|b||b|b|b|b|||b|b"
                 rowString = objKey + '|' + objResult[12] + '|' + objResult[13] + '|' + objResult[14] + '|' + objResult[15] + '\r\n';
                 fileString += rowString; // "b|b|b||b|b|b|b|||b|b"'\r\n' "b|b|b||b|b|b|b|||b|b"'\r\n'
                 fileSize += lengthInUtf8Bytes(rowString);

                 if (fileSize > 9000000) {
                     if (fileNumber == 0) {
                         filesId = saveAuxiliaryFile(fileString, fileNumber, 1);
                     } else {
                         filesId = filesId + "|" + saveAuxiliaryFile(fileString, fileNumber);
                     }
                     fileString = "";
                     fileSize = 0;
                     fileNumber++;
                 }
             }
             return true;
         });

         if (errors.length > 0) {
             log.error("error", errors);
             UnexError();
         } else {
             log.error("fileString", fileString);
             if (fileString.length != 0) {
                 if (fileNumber == 0) {
                     filesId = saveAuxiliaryFile(fileString, fileNumber);
                 } else {
                     filesId = filesId + "|" + saveAuxiliaryFile(fileString, fileNumber);
                 }
             }
             callMapReducecript(filesId);

             log.error("termino summarize");

         }

     } catch (error) {
         UnexError();
         log.error("error", error);
     }
 }

 function getTaxResultFiltered(objResult) {
     //var objResult = ['VendBill', '5425493', 2016, '7155', 'BILL EXTRANJERO']
     log.error('objResult line', objResult);

     var tax_result_actualizado = [];
     try {
         var idOrigen = objResult['internalid'];

         //TODO: OBTENER TAX RECLASIFICADO
         var tax_result_reclasf = obtenerTaxReclasificado(idOrigen);
         //TODO: ACTUALIZA LOS PERIODOS con tax_result_reclasf

         if (tax_result_reclasf[objResult['taxResultId']] != undefined) {
             objResult['taxResult'][9] = tax_result_reclasf[objResult['taxResultId']];

             var year = format.parse({
                 value: tax_result_reclasf[objResult['taxResultId']],
                 type: format.Type.DATE
             }).getFullYear();

             objResult['year'] = year;
         }
         if (objResult['year'] == paramPeriod) {
             tax_result_actualizado = objResult['taxResult'];
         }
     } catch (error) {
         log.debug("FIX ME", error);
         log.debug("FIX getTaxResultFiltered", objResult);
     }

     log.error('tax_result_actualizado', tax_result_actualizado);

     return tax_result_actualizado;
 }

 function obtenerTaxReclasificado(idOrigen) {

     var customrecord_lmry_co_wht_reclasification_search = search.create({
         type: "customrecord_lmry_co_wht_reclasification",
         filters: [
             ["custrecord_co_reclasification_data", "contains", idOrigen],
             "AND", ["custrecord_co_reclasification_status", "is", "Complete"],
             "AND", ["custrecord_co_reclasification_return", "isnot", "{}"]
         ],
         columns: [
             search.createColumn({
                 name: "formulatext",
                 formula: "{custrecord_co_reclasification_return}",
                 label: "Formula (Text)"
             }),
             search.createColumn({
                 name: "formuladate",
                 formula: "{custrecord_co_reclasification_condate}",
                 label: "Formula (Date)"
             })
         ]
     });

     var pagedData = customrecord_lmry_co_wht_reclasification_search.runPaged({
         pageSize: 1000,
     });

     var page,
         transactionsArray = [];

     pagedData.pageRanges.forEach(function(pageRange) {
         page = pagedData.fetch({
             index: pageRange.index,
         });

         page.data.forEach(function(result) {
             if (result.getValue(result.columns[0]).split('null').length < 2) {
                 var jsonTaxRe_aux = {};
                 var date = '';
                 // 0. DATA
                 if (result.getValue(result.columns[0]) != "- None -" && result.getValue(result.columns[0]) != "" && result.getValue(result.columns[0]) != null) {
                     jsonTaxRe_aux = JSON.parse(result.getValue(result.columns[0]));
                 } else {
                     jsonTaxRe_aux = {};
                 }

                 //1 . ID TRANSACCION ORIGEN
                 if (result.getValue(result.columns[1]) != "- None -" && result.getValue(result.columns[1]) != "" && result.getValue(result.columns[1]) != null) {
                     date = result.getValue(result.columns[1]);
                 } else {
                     date = '';
                 }

                 jsonTaxRe_aux['date'] = date;

                 transactionsArray.push(jsonTaxRe_aux);

             }
         });
     });

     var json_final = {};

     for (var i = 0; i < transactionsArray.length; i++) {
         var aux_tax = transactionsArray[i][idOrigen];
         for (var j = 0; j < aux_tax.length; j++) {
             //arr_final.push[aux_tax[j]['taxResult'],transactionsArray[i]['year']]
             json_final[aux_tax[j]['taxResult'] + ''] = transactionsArray[i]['date'];
         }
     }

     return json_final;
 }

 function UnexError() {

     var usuarioTemp = runtime.getCurrentUser();
     var id = usuarioTemp.id;
     var employeename = search.lookupFields({
         type: search.Type.EMPLOYEE,
         id: id,
         columns: ['firstname', 'lastname']
     });
     var usuario = employeename.firstname + ' ' + employeename.lastname;

     var recordLog = record.load({
         type: 'customrecord_lmry_co_rpt_generator_log',
         id: paramLogId
     });

     var GLOBAL_LABELS = {
         'noData2': {
             "es": 'Ocurrio un error inesperado en la ejecucion del reporte.',
             "pt": 'Ocorreu um erro inesperado ao executar o relatório.',
             "en": 'An unexpected error occurred while executing the report.'
         }
     }

     var mensaje = GLOBAL_LABELS['noData2'][language];


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

 function getJournalsAMano() {

     if (hasMultibookFeature) {
         var isPrimary = IsPrimary(paramSubsidiary, paramMultibook);
         //log.error('isPrimary', isPrimary);
     } else {
         var isPrimary = true;
     }

     var savedSearch = search.create({
         type: "transaction",
         filters: [
             ["type", "anyof", "Journal"],
             "AND", ["formulatext: CASE WHEN {lineuniquekey} = {custrecord_lmry_br_transaction.custrecord_lmry_lineuniquekey} THEN 1 ELSE 0 END", "is", "1"],
             "AND", ["mainline", "is", "T"],
             "AND", ["formulatext: CASE WHEN NVL({custrecord_lmry_br_transaction.custrecord_lmry_br_type},'') = 'ReteFTE' OR NVL({custrecord_lmry_br_transaction.custrecord_lmry_br_type},'') = 'ReteIVA' OR NVL({custrecord_lmry_br_transaction.custrecord_lmry_br_type},'') = 'Auto ReteIVA' OR NVL({custrecord_lmry_br_transaction.custrecord_lmry_br_type},'') = 'Auto ReteFTE' THEN 1 ELSE 0 END", "is", "1"],
             "AND", ["formulatext: CASE WHEN NVL({custrecord_lmry_br_transaction.custrecord_lmry_tax_type},'') = 'Retencion' THEN 1 ELSE 0 END", "is", "1"],
             "AND", ["posting", "is", "T"],
             "AND", ["voided", "is", "F"],
             "AND", ["memorized", "is", "F"]
         ],
         columns: [
             search.createColumn({ name: "type", label: " 0. Type" }),
             search.createColumn({
                 name: "formulatext",
                 formula: "{custrecord_lmry_br_transaction.custrecord_lmry_br_type}",
                 label: " 1. Sub Type"
             }),
             search.createColumn({
                 name: "formulatext",
                 formula: "{custrecord_lmry_br_transaction.custrecord_lmry_ccl.id}",
                 label: "2. Contributory Class"
             }),
             search.createColumn({
                 name: "formulatext",
                 formula: "{custrecord_lmry_br_transaction.custrecord_lmry_ntax.id}",
                 label: "3. N Tax"
             }),
             search.createColumn({
                 name: "formulatext",
                 formula: "{custrecord_lmry_br_transaction.custrecord_lmry_amount_local_currency}",
                 label: "4. Total"
             }),
             search.createColumn({
                 name: "formulatext",
                 formula: "{custrecord_lmry_br_transaction.custrecord_lmry_accounting_books}",
                 label: "5. ACCOUNTING BOOKS"
             })
         ]
     });

     if (paramPeriod) {

         /* var periodStartDate = format.format({
             type: format.Type.DATE,
             value: new Date(paramPeriod, 0, 1)
         });

         var periodEndDate = format.format({
             type: format.Type.DATE,
             value: new Date(paramPeriod, 11, 31)
         }); */

         var arrayPeriods = getArrayPeriods(paramPeriod, paramSubsidiary);
         var formulPeriodFilters = getFormulPeriodsFilters(arrayPeriods);

         var periodFilter = search.createFilter({
            name: "formulatext",
            formula: formulPeriodFilters,
            operator: search.Operator.IS,
            values: "1"
        });

         savedSearch.filters.push(periodFilter);

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
         formula: 'NVL({vendor.internalid},{vendorline.internalid})'

     });
     savedSearch.columns.push(vendorColumn);

     if (hasJobsFeature && !hasAdvancedJobsFeature) {
         var customerColumn = search.createColumn({
             name: 'formulanumeric',
             formula: 'NVL({customermain.internalid},{customer.internalid})'
         });
         savedSearch.columns.push(customerColumn);
     } else if ((!hasJobsFeature && !hasAdvancedJobsFeature) || (!hasJobsFeature && hasAdvancedJobsFeature) || (hasJobsFeature && hasAdvancedJobsFeature)) {
         var customerColumn = search.createColumn({
             name: "formulanumeric",
             formula: "CASE WHEN NVL({job.internalid},-1) = -1 THEN {customer.internalid} ELSE {job.customer.id} end"
         });
         savedSearch.columns.push(customerColumn);
     }

     var DateBill = search.createColumn({
         name: "trandate",
         label: "Date"
     });
     savedSearch.columns.push(DateBill);

     if (hasMultibookFeature) {
         var multibookFilter = search.createFilter({
             name: 'accountingbook',
             join: 'accountingtransaction',
             operator: search.Operator.IS,
             values: [paramMultibook]
         });
         savedSearch.filters.push(multibookFilter);

         var accountIdColumn = search.createColumn({
             name: 'formulanumeric',
             formula: '{accountingtransaction.account.id}',
         });
         savedSearch.columns.push(accountIdColumn);
     } else {
         var account = search.createColumn({
             name: "formulanumeric",
             formula: '{account.internalid}'
         });
         savedSearch.columns.push(account);
     }
     var pagedData = savedSearch.runPaged({
         pageSize: 1000
     });

     var page, auxArray, transactionsArray = [];
     var cont = 0;


     pagedData.pageRanges.forEach(function(pageRange) {

         page = pagedData.fetch({
             index: pageRange.index
         });
         page.data.forEach(function(result) {
             auxArray = [];
             cont++;
             auxArray[0] = '';

             // 1. Tipo
             if (result.getValue(result.columns[0]) != '- None -' && result.getValue(result.columns[0]) != '') {
                 auxArray[1] = result.getValue(result.columns[0]);
                 if (auxArray[1] == 'VendBill' || auxArray[1] == 'VendCred') {
                     auxArray[0] = 'EXPENSE';
                 } else if (auxArray[1] == 'CustCred' || auxArray[1] == 'CustInvc') {
                     auxArray[0] = 'INCOME';
                 } else {
                     auxArray[0] = 'JOURNAL';
                 }
             } else {
                 auxArray[1] = '';
             }

             // 2. Tipo de Retención - Sub Type (ReteIVA o ReteFTE)
             if (result.getValue(result.columns[1]) != '- None -' && result.getValue(result.columns[1]) != '') {
                 auxArray[2] = result.getValue(result.columns[1]);
             } else {
                 auxArray[2] = '';
             }

             // 3. Clase Contributiva
             if (result.getValue(result.columns[2]) != '- None -' && result.getValue(result.columns[2]) != '') {
                 auxArray[3] = result.getValue(result.columns[2]);
             } else {
                 auxArray[3] = '';
             }

             // 4. National Tax
             if (result.getValue(result.columns[3]) != '- None -' && result.getValue(result.columns[3]) != '') {
                 auxArray[4] = result.getValue(result.columns[3]);
             } else {
                 auxArray[4] = '';
             }

             // 5. Accounting Books
             if (result.getValue(result.columns[5]) != '- None -' && result.getValue(result.columns[5]) != '') {
                 auxArray[5] = result.getValue(result.columns[5]);
             } else {
                 auxArray[5] = '';
             }

             // 6. Total
             if (result.getValue(result.columns[4]) != '- None -' && result.getValue(result.columns[4]) != '') {
                 auxArray[6] = signAmount(auxArray[1], result.getValue(result.columns[4]), '');
             } else {
                 auxArray[6] = 0;
             }

             // 7. Id Vendor
             if (result.getValue(result.columns[6]) != '- None -' && result.getValue(result.columns[6]) != '') {
                 auxArray[7] = result.getValue(result.columns[6]);
             } else {
                 auxArray[7] = '';
             }
             // 8. Id Customer
             if (result.getValue(result.columns[7]) != '- None -' && result.getValue(result.columns[7]) != '') {
                 auxArray[8] = result.getValue(result.columns[7]);
             } else {
                 auxArray[8] = '';
             }
             // 9. Date
             if (result.getValue(result.columns[8]) != '- None -' && result.getValue(result.columns[8]) != '') {
                 auxArray[9] = result.getValue(result.columns[8]);
             } else {
                 auxArray[9] = '';
             }
             // 10. Account
             if (result.getValue(result.columns[9]) != '- None -' && result.getValue(result.columns[9]) != '') {
                 auxArray[10] = result.getValue(result.columns[9]);
             } else {
                 auxArray[10] = '';
             }
             auxArray[11] = isPrimary; // el valor booleano obtenido no participa en alguna busqueda o validacion
             auxArray[12] = 'journalsaMAno';

             transactionsArray.push(auxArray);
         });
     });

     return transactionsArray;
 }

 function getIdOrigenTransacctionLine() {

     var savedSearch = search.load({
         id: 'customsearch_lmry_co_ret1001_lineid_mprd'
     });

     if (paramPeriod) {

        /* var periodStartDate = format.format({
                type: format.Type.DATE,
                value: new Date(paramPeriod, 0, 1)
            });
            var periodEndDate = format.format({
                type: format.Type.DATE,
                value: new Date(paramPeriod, 11, 31)
            }); */

        var arrayPeriods = getArrayPeriods(paramPeriod, paramSubsidiary);
        var formulPeriodFilters = getFormulPeriodsFilters(arrayPeriods);

        var periodFilter = search.createFilter({
            name: "formulatext",
            formula: formulPeriodFilters,
            operator: search.Operator.IS,
            values: "1"
        });
        savedSearch.filters.push(periodFilter);

     }

     if (hasSubsidiariesFeature) {
         var subsidiaryFilter = search.createFilter({
             name: 'subsidiary',
             operator: search.Operator.IS,
             values: paramSubsidiary
         });
         savedSearch.filters.push(subsidiaryFilter);
     }

     var pagedData = savedSearch.runPaged({
         pageSize: 1000
     });

     var page, transactionsArray = [];

     pagedData.pageRanges.forEach(function(pageRange) {

         page = pagedData.fetch({
             index: pageRange.index
         });
         page.data.forEach(function(result) {

             var id_aux = '';

             // 2. Tipo de Retención - Sub Type (ReteIVA o ReteFTE)
             if (result.getValue(result.columns[0]) != '- None -' && result.getValue(result.columns[0]) != '') {
                 id_aux += result.getValue(result.columns[0]);
             } else {
                 id_aux = '';
             }

             transactionsArray.push(id_aux);
         });
     });
     return transactionsArray;
 }

 function getCreditNotes() {

     var arrIdOrigen = getIdOrigenTransacctionLine();
     var transactionsArray = [];
     log.debug('arrIdOrigen LINE', arrIdOrigen.length);

     if (hasMultibookFeature) {
         var isPrimary = IsPrimary(paramSubsidiary, paramMultibook);
         //log.error('isPrimary', isPrimary);
     } else {
         var isPrimary = true;
     }

     if (arrIdOrigen.length) {
         var savedSearch = search.load({
             id: 'customsearch_lmry_co_form_1001_reten_v11'
         });

         var internalidFilter = search.createFilter({
             name: 'internalid',
             operator: search.Operator.ANYOF,
             values: arrIdOrigen
         });
         savedSearch.filters.push(internalidFilter);
         //6 columnas

         var vendorColumn = search.createColumn({
             name: 'formulanumeric',
             formula: 'NVL({vendor.internalid},{vendorline.internalid})'

         });
         savedSearch.columns.push(vendorColumn);

         if (hasJobsFeature && !hasAdvancedJobsFeature) {
             var customerColumn = search.createColumn({
                 name: 'formulanumeric',
                 formula: 'NVL({customermain.internalid},{customer.internalid})'
             });
             savedSearch.columns.push(customerColumn);
         } else if ((!hasJobsFeature && !hasAdvancedJobsFeature) || (!hasJobsFeature && hasAdvancedJobsFeature) || (hasJobsFeature && hasAdvancedJobsFeature)) {
             var customerColumn = search.createColumn({
                 name: "formulanumeric",
                 formula: "CASE WHEN NVL({job.internalid},-1) = -1 THEN {customer.internalid} ELSE {job.customer.id} end"
             });
             savedSearch.columns.push(customerColumn);
         }

         var DateBill = search.createColumn({
             name: "trandate",
             label: "Date"
         });
         savedSearch.columns.push(DateBill);

         if (hasMultibookFeature) {
             var multibookFilter = search.createFilter({
                 name: 'accountingbook',
                 join: 'accountingtransaction',
                 operator: search.Operator.IS,
                 values: [paramMultibook]
             });
             savedSearch.filters.push(multibookFilter);

             var accountIdColumn = search.createColumn({
                 name: 'formulanumeric',
                 formula: '{accountingtransaction.account.id}',
             });
             savedSearch.columns.push(accountIdColumn);
         } else {
             var account = search.createColumn({
                 name: "formulanumeric",
                 formula: '{account.internalid}'
             });
             savedSearch.columns.push(account);
         }
         // 10
         var internalid = search.createColumn({
             name: 'formulanumeric',
             formula: '{internalid}'
         });
         savedSearch.columns.push(internalid);

         // 11
         var internalidTax = search.createColumn({
             name: 'formulanumeric',
             formula: '{custrecord_lmry_br_transaction.internalid}'
         });
         savedSearch.columns.push(internalidTax);

         //Se agregan las columnas de vendor, customer, la fecha de creacion de la factura y su cuenta
         //10 columnas
         var pagedData = savedSearch.runPaged({
             pageSize: 1000
         });

         var page, auxArray;
         var cont = 0;


         pagedData.pageRanges.forEach(function(pageRange) {

             page = pagedData.fetch({
                 index: pageRange.index
             });
             page.data.forEach(function(result) {
                 auxArray = [];
                 cont++;
                 auxArray[0] = '';

                 // 1. Tipo
                 if (result.getValue(result.columns[0]) != '- None -' && result.getValue(result.columns[0]) != '') {
                     auxArray[1] = result.getValue(result.columns[0]);
                     if (auxArray[1] == 'VendBill' || auxArray[1] == 'VendCred') {
                         auxArray[0] = 'EXPENSE';
                     } else if (auxArray[1] == 'CustCred' || auxArray[1] == 'CustInvc') {
                         auxArray[0] = 'INCOME';
                     } else {
                         auxArray[0] = 'JOURNAL';
                     }
                 } else {
                     auxArray[1] = '';
                 }

                 // 2. Tipo de Retención - Sub Type (ReteIVA o ReteFTE)
                 if (result.getValue(result.columns[1]) != '- None -' && result.getValue(result.columns[1]) != '') {
                     auxArray[2] = result.getValue(result.columns[1]);
                 } else {
                     auxArray[2] = '';
                 }

                 // 3. Clase Contributiva
                 if (result.getValue(result.columns[2]) != '- None -' && result.getValue(result.columns[2]) != '') {
                     auxArray[3] = result.getValue(result.columns[2]);
                 } else {
                     auxArray[3] = '';
                 }

                 // 4. National Tax
                 if (result.getValue(result.columns[3]) != '- None -' && result.getValue(result.columns[3]) != '') {
                     auxArray[4] = result.getValue(result.columns[3]);
                 } else {
                     auxArray[4] = '';
                 }

                 // 5. Accounting Books
                 if (result.getValue(result.columns[5]) != '- None -' && result.getValue(result.columns[5]) != '') {
                     auxArray[5] = result.getValue(result.columns[5]);
                 } else {
                     auxArray[5] = '';
                 }

                 // 6. Total
                 if (result.getValue(result.columns[4]) != '- None -' && result.getValue(result.columns[4]) != '') {
                     auxArray[6] = signAmount(auxArray[1], result.getValue(result.columns[4]), '');
                 } else {
                     auxArray[6] = 0;
                 }

                 // 7. Id Vendor
                 if (result.getValue(result.columns[6]) != '- None -' && result.getValue(result.columns[6]) != '') {
                     auxArray[7] = result.getValue(result.columns[6]);
                 } else {
                     auxArray[7] = '';
                 }
                 // 8. Id Customer
                 if (result.getValue(result.columns[7]) != '- None -' && result.getValue(result.columns[7]) != '') {
                     auxArray[8] = result.getValue(result.columns[7]);
                 } else {
                     auxArray[8] = '';
                 }
                 // 9. Date
                 if (result.getValue(result.columns[8]) != '- None -' && result.getValue(result.columns[8]) != '') {
                     auxArray[9] = result.getValue(result.columns[8]);
                 } else {
                     auxArray[9] = '';
                 }
                 // 10. Account
                 if (result.getValue(result.columns[9]) != '- None -' && result.getValue(result.columns[9]) != '') {
                     auxArray[10] = result.getValue(result.columns[9]);
                 } else {
                     auxArray[10] = '';
                 }

                 auxArray[11] = isPrimary;  // el valor booleano obtenido no participa en alguna busqueda o validacion
                 auxArray[12] = 'xLineas';

                 var year = format.parse({
                     value: auxArray[9],
                     type: format.Type.DATE
                 }).getFullYear();

                 var jsonAux = {};
                 jsonAux['internalid'] = result.getValue(result.columns[10]);
                 jsonAux['taxResultId'] = result.getValue(result.columns[11]);
                 jsonAux['taxResult'] = auxArray;
                 jsonAux['type'] = 'xLineas';
                 jsonAux['year'] = year;

                 transactionsArray.push(jsonAux);
             });
         });
     }

     return transactionsArray;
 }

 function getIdOrigenTransacctionMain() {

     var savedSearch = search.load({
         id: 'customsearch_lmry_co_ret1001_mainid_mprd'
     });

     if (paramPeriod) {

         /* var periodStartDate = format.format({
             type: format.Type.DATE,
             value: new Date(paramPeriod, 0, 1)
         });

         var periodEndDate = format.format({
             type: format.Type.DATE,
             value: new Date(paramPeriod, 11, 31)
         }); */

         var arrayPeriods = getArrayPeriods(paramPeriod, paramSubsidiary);
         var formulPeriodFilters = getFormulPeriodsFilters(arrayPeriods);
         var periodFilter = search.createFilter({
            name: "formulatext",
            formula: formulPeriodFilters,
            operator: search.Operator.IS,
            values: "1"
         });

         savedSearch.filters.push(periodFilter);

     }

     if (hasSubsidiariesFeature) {
         var subsidiaryFilter = search.createFilter({
             name: 'subsidiary',
             operator: search.Operator.IS,
             values: paramSubsidiary
         });
         savedSearch.filters.push(subsidiaryFilter);
     }

     var pagedData = savedSearch.runPaged({
         pageSize: 1000
     });

     var page, transactionsArray = [];
     var json_final_main = {}
     pagedData.pageRanges.forEach(function(pageRange) {

         page = pagedData.fetch({
             index: pageRange.index
         });
         page.data.forEach(function(result) {


             // 0. Tipo de Retención - Sub Type (ReteIVA o ReteFTE)
             var id_origen_aux = '';
             if (result.getValue(result.columns[0]) != '- None -' && result.getValue(result.columns[0]) != '') {
                 id_origen_aux += result.getValue(result.columns[0]);
             } else {
                 id_origen_aux = '';
             }

             // 1. Tipo de Retención - Sub Type (ReteIVA o ReteFTE)
             if (result.getValue(result.columns[1]) != '- None -' && result.getValue(result.columns[1]) != '') {
                 var memo_aux = result.getValue(result.columns[1]);
             } else {
                 var memo_aux = '';
             }

             // 2. Tipo de Retención - Sub Type (ReteIVA o ReteFTE)
             if (result.getValue(result.columns[2]) != '- None -' && result.getValue(result.columns[2]) != '') {
                 var date_aux = result.getValue(result.columns[2]);
             } else {
                 var date_aux = '';
             }

             if (json_final_main[id_origen_aux] == undefined) {
                 json_final_main[id_origen_aux] = {}
                 json_final_main[id_origen_aux]['memo'] = [memo_aux];
                 json_final_main[id_origen_aux]['trandate'] = [date_aux];
             } else {
                 json_final_main[id_origen_aux]['memo'].push(memo_aux);
                 json_final_main[id_origen_aux]['trandate'].push(date_aux);
             }
         });
     });
     return json_final_main;
 }

 function MaingetCreditNotes() {

     var jsonIdOrigen = getIdOrigenTransacctionMain();
     var arrIdOrigen = Object.keys(jsonIdOrigen);
     var transactionsArray = [];
     log.debug('arrIdOrigen Main', arrIdOrigen.length);

     if (hasMultibookFeature) {
         var isPrimary = IsPrimary(paramSubsidiary, paramMultibook);
         //log.error('isPrimary', isPrimary);
     } else {
         var isPrimary = true;
     }

     if (arrIdOrigen.length) {

         var savedSearch = search.load({
             id: 'customsearch_lmry_co_form_1001_ret_main'
         });

         var internalidFilter = search.createFilter({
             name: 'internalid',
             operator: search.Operator.ANYOF,
             values: arrIdOrigen
         });
         savedSearch.filters.push(internalidFilter);

         if (hasSubsidiariesFeature) {
             var subsidiaryFilter = search.createFilter({
                 name: 'subsidiary',
                 operator: search.Operator.IS,
                 values: paramSubsidiary
             });
             savedSearch.filters.push(subsidiaryFilter);
         }

         var DateBill = search.createColumn({
             name: "trandate",
             label: "Date"
         });
         savedSearch.columns.push(DateBill);

         var vendorColumn = search.createColumn({
             name: 'formulanumeric',
             formula: 'NVL({vendor.internalid},{vendorline.internalid})'

         });
         savedSearch.columns.push(vendorColumn);

         if (hasJobsFeature && !hasAdvancedJobsFeature) {
             var customerColumn = search.createColumn({
                 name: 'formulanumeric',
                 formula: 'NVL({customermain.internalid},{customer.internalid})'
             });
             savedSearch.columns.push(customerColumn);
         } else if ((!hasJobsFeature && !hasAdvancedJobsFeature) || (!hasJobsFeature && hasAdvancedJobsFeature) || (hasJobsFeature && hasAdvancedJobsFeature)) {
             var customerColumn = search.createColumn({
                 name: "formulanumeric",
                 formula: "CASE WHEN NVL({job.internalid},-1) = -1 THEN {customer.internalid} ELSE {job.customer.id} end"
             });
             savedSearch.columns.push(customerColumn);
         }
         //16
         var memoReteIva = search.createColumn({
             name: 'formulatext',
             formula: '{custbody_lmry_co_reteiva.name}'

         });
         savedSearch.columns.push(memoReteIva);
         //17
         var memoReteFte = search.createColumn({
             name: 'formulatext',
             formula: '{custbody_lmry_co_retefte.name}'

         });
         savedSearch.columns.push(memoReteFte);

         // 18
         var internalid = search.createColumn({
             name: 'formulanumeric',
             formula: '{internalid}'
         });
         savedSearch.columns.push(internalid);

         var pagedData = savedSearch.runPaged({
             pageSize: 1000
         });

         var page, auxArray;
         var cont = 0;

         pagedData.pageRanges.forEach(function(pageRange) {

             page = pagedData.fetch({
                 index: pageRange.index
             });
             page.data.forEach(function(result) {
                 auxArray = [];
                 cont++;

                 // 0. Tipo
                 if (result.getValue(result.columns[0]) != '- None -' && result.getValue(result.columns[0]) != '') {
                     auxArray[0] = result.getValue(result.columns[0]);
                 } else {
                     auxArray[0] = '';
                 }

                 // 1. ReteIVA Amount
                 if (result.getValue(result.columns[1]) != '- None -' && result.getValue(result.columns[1]) != '') {
                     auxArray[1] = signAmount(auxArray[0], result.getValue(result.columns[1]), '');
                 } else {
                     auxArray[1] = '';
                 }

                 // 2. ReteFTE Amount
                 if (result.getValue(result.columns[2]) != '- None -' && result.getValue(result.columns[2]) != '') {
                     auxArray[2] = signAmount(auxArray[0], result.getValue(result.columns[2]), '');
                 } else {
                     auxArray[2] = '';
                 }

                 // 3. ReteIVA Type WHT
                 if (result.getValue(result.columns[3]) != '- None -' && result.getValue(result.columns[3]) != '') {
                     auxArray[3] = result.getValue(result.columns[3]);
                 } else {
                     auxArray[3] = '';
                 }

                 // 4. ReteFTE Type WHT
                 if (result.getValue(result.columns[4]) != '- None -' && result.getValue(result.columns[4]) != '') {
                     auxArray[4] = result.getValue(result.columns[4]);
                 } else {
                     auxArray[4] = '';
                 }

                 // 5. ReteIVA Account Sales
                 if (result.getValue(result.columns[5]) != '- None -' && result.getValue(result.columns[5]) != '') {
                     auxArray[5] = result.getValue(result.columns[5]);
                 } else {
                     auxArray[5] = '';
                 }

                 // 6. ReteIVA Account Purchase
                 if (result.getValue(result.columns[6]) != '- None -' && result.getValue(result.columns[6]) != '') {
                     auxArray[6] = result.getValue(result.columns[6]);
                 } else {
                     auxArray[6] = '';
                 }
                 // 7. ReteFTE Account Sales
                 if (result.getValue(result.columns[7]) != '- None -' && result.getValue(result.columns[7]) != '') {
                     auxArray[7] = result.getValue(result.columns[7]);
                 } else {
                     auxArray[7] = '';
                 }
                 // 8. ReteFTE Account Purchase
                 if (result.getValue(result.columns[8]) != '- None -' && result.getValue(result.columns[8]) != '') {
                     auxArray[8] = result.getValue(result.columns[8]);
                 } else {
                     auxArray[8] = '';
                 }
                 // 9. ReteIVA Credit Acc
                 if (result.getValue(result.columns[9]) != '- None -' && result.getValue(result.columns[9]) != '') {
                     auxArray[9] = result.getValue(result.columns[9]);
                 } else {
                     auxArray[9] = '';
                 }
                 // 10. ReteIVA ToPay Acc
                 if (result.getValue(result.columns[10]) != '- None -' && result.getValue(result.columns[10]) != '') {
                     auxArray[10] = result.getValue(result.columns[10]);
                 } else {
                     auxArray[10] = '';
                 }
                 // 11. ReteFTE Credit Acc
                 if (result.getValue(result.columns[11]) != '- None -' && result.getValue(result.columns[11]) != '') {
                     auxArray[11] = result.getValue(result.columns[11]);
                 } else {
                     auxArray[11] = '';
                 }
                 // 12. ReteFTE ToPay Acc
                 if (result.getValue(result.columns[12]) != '- None -' && result.getValue(result.columns[12]) != '') {
                     auxArray[12] = result.getValue(result.columns[12]);
                 } else {
                     auxArray[12] = '';
                 }
                 // 13. Date
                 // if (result.getValue(result.columns[13]) != '- None -' && result.getValue(result.columns[13]) != '') {
                 //     auxArray[13] = result.getValue(result.columns[13]);
                 // } else {
                 auxArray[13] = '';
                 //}
                 // 14. Vendor
                 if (result.getValue(result.columns[14]) != '- None -' && result.getValue(result.columns[14]) != '') {
                     auxArray[14] = result.getValue(result.columns[14]);
                 } else {
                     auxArray[14] = '';
                 }
                 // 15. Customer
                 if (result.getValue(result.columns[15]) != '- None -' && result.getValue(result.columns[15]) != '') {
                     auxArray[15] = result.getValue(result.columns[15]);
                 } else {
                     auxArray[15] = '';
                 }
                 // 16. ES LIBRO PRIMARIO?
                 auxArray[16] = isPrimary;  // el valor booleano obtenido no participa en alguna busqueda o validacion
                 //* NOTA: EN PRINCIPIO, AMBAS FECHAS SON IGUALES, TRANDATE IVA = TRANDATE FTE
                 // 17. TRANDATE IVA 
                 if (result.getValue(result.columns[13]) != '- None -' && result.getValue(result.columns[13]) != '') {
                     auxArray[17] = result.getValue(result.columns[13]);
                 } else {
                     auxArray[17] = '';
                 }
                 // 18. TRANDATE FTE
                 auxArray[18] = auxArray[17];

                 //*SIN EMBARGO, EN CASO QUE EXISTA UNA RECLASIFICACIÓN ESTAS FECHAS CAMBIARAN , MAS NO, LA FECHA DE TRANSACCION ORIGEN
                 //*SE DEBE ACTUALIZAR LA FECHA SEGUN LAS RETENCIONES DE CABECERA
                 var id_origen_aux = result.getValue(result.columns[18]);
                 //*IVA
                 var memoiva_aux = result.getValue(result.columns[16]);
                 var posMemoIva = jsonIdOrigen[id_origen_aux]['memo'].indexOf(memoiva_aux);
                 auxArray[17] = posMemoIva > -1 ? jsonIdOrigen[id_origen_aux]['trandate'][posMemoIva] : auxArray[17];
                 //*FTE
                 var memofte_aux = result.getValue(result.columns[17]);
                 var posMemoFte = jsonIdOrigen[id_origen_aux]['memo'].indexOf(memofte_aux);
                 auxArray[18] = posMemoFte > -1 ? jsonIdOrigen[id_origen_aux]['trandate'][posMemoFte] : auxArray[18];

                 var yearIVA = format.parse({
                     value: auxArray[17],
                     type: format.Type.DATE
                 }).getFullYear();
                 //* SI PERTENECE A OTRO PERIODO, MONTO = 0 
                 if (yearIVA != paramPeriod) {
                     auxArray[1] = 0;
                 }

                 var yearFTE = format.parse({
                     value: auxArray[18],
                     type: format.Type.DATE
                 }).getFullYear();
                 //* SI PERTENECE A OTRO PERIODO, MONTO = 0 
                 if (yearFTE != paramPeriod) {
                     auxArray[2] = 0;
                 }

                 auxArray[19] = result.getValue(result.columns[18]);

                 auxArray[20] = 'xTotales';

                 transactionsArray.push(auxArray);
             });
         });
     }

     return transactionsArray;
 }


 function getCreditNoteAccountsDetail(contextValue) {

     try {

         var TypeAppTransaction = '';
         var accountsDetailJson = {};

         //log.error('contextValue', contextValue);

         if (contextValue[1] == 'Journal') {

             var account = contextValue[10];
         } else {

             var objResult = ObtieneAccxCContri_o_NTax(contextValue[0], contextValue[3], contextValue[4]);
             //log.error('objResult', objResult);

             if (objResult && objResult.length) {

                 var columns = objResult[0].columns;
                 TypeAppTransaction = objResult[0].getValue(columns[1]);

                 //log.error('TypeAppTransaction', TypeAppTransaction);

                 if (TypeAppTransaction == 'Journal' || (TypeAppTransaction == 'WHT by Transaction' && (contextValue[2] == 'Auto ReteFTE' || contextValue[2] == 'Auto ReteIVA'))) {
                     var creditAcc = objResult[0].getValue(columns[2]);
                     var debitAcc = objResult[0].getValue(columns[3]);
                     var account = [creditAcc, debitAcc];
                 } else if (TypeAppTransaction == 'WHT by Transaction' && (contextValue[2] == 'ReteFTE' || contextValue[2] == 'ReteIVA')) {
                     var account = objResult[0].getValue(columns[0]);
                 }
             }
         }

         //log.error('account', account);

         if (TypeAppTransaction == 'Journal' || TypeAppTransaction == 'WHT by Transaction' || contextValue[1] == 'Journal') {
             var newSearch = search.create({

                 type: 'account',
                 filters: [
                     ['internalid', 'anyof', account],
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
                     }),
                     search.createColumn({
                         name: "custrecord_lmry_co_fields_format_c",
                         join: "custrecord_lmry_co_puc_concept"
                     })
                 ]
             });

             var objResult_2 = newSearch.run().getRange(0, 1000);
             var countriesJson = getCountries();

             var resultArray = [],
                 key = '';

             //log.error('Tamaño', objResult_2.length);

             if (objResult_2 && objResult_2.length) {

                 for (var j = 0; j < objResult_2.length; j++) {
                     resultArray = [];
                     columns = objResult_2[j].columns;

                     // Concepto
                     resultArray[0] = objResult_2[j].getValue(columns[1]);

                     var aux_info = getInformation(contextValue[1], contextValue[7], contextValue[8]);

                     if (aux_info.length != 0) {

                         var codeCountry = countriesJson[aux_info[10]];
                         resultArray = resultArray.concat(aux_info);

                         var entityType = '',
                             entityId = '';



                         if (contextValue[1] == 'VendBill' || contextValue[1] == 'VendCred') {
                             entityType = 'vendor';
                             entityId = contextValue[7];
                         } else if (contextValue[1] == 'CustCred' || contextValue[1] == 'CustInvc') {
                             entityType = 'customer';
                             entityId = contextValue[8];
                         } else if (contextValue[1] == 'Journal') {
                             if (contextValue[7] != '') {
                                 entityType = 'vendor';
                                 entityId = contextValue[7];
                             } else {
                                 entityType = 'customer';
                                 entityId = contextValue[8];
                             }
                         }

                         // 12, 13 14 y 15
                         resultArray[12] = 0;
                         resultArray[13] = 0;
                         resultArray[14] = 0;
                         resultArray[15] = 0;

                         var typeOfRet = objResult_2[j].getValue(columns[2]).split(',');

                         var ReteFteTipo1_2 = (TypeAppTransaction == 'WHT by Transaction' && contextValue[2] == 'ReteFTE'); //No cambia la forma de roconocer la retencion el la v1 y la v2
                         var autoReteFteTipo1 = ((TypeAppTransaction == 'Journal' || contextValue[1] == 'Journal') && contextValue[2] == 'ReteFTE');;
                         var autoReteFteTipo2 = (TypeAppTransaction == 'WHT by Transaction' && contextValue[2] == 'Auto ReteFTE');

                         var ReteIvaTipo1_2 = (TypeAppTransaction == 'WHT by Transaction' && contextValue[2] == 'ReteIVA');
                         var autoReteIvaTipo1 = ((TypeAppTransaction == 'Journal' || contextValue[1] == 'Journal') && contextValue[2] == 'ReteIVA');
                         var autoReteIvaTipo2 = (TypeAppTransaction == 'WHT by Transaction' && contextValue[2] == 'Auto ReteIVA');

                         if (ReteFteTipo1_2 && typeOfRet.indexOf('5') >= 0) {
                             resultArray[12] = contextValue[6];
                         } else if ((autoReteFteTipo1 || autoReteFteTipo2) && typeOfRet.indexOf('6') >= 0) {
                            if (contextValue[1] == 'VendBill') {
                                resultArray[13] = contextValue[6];
                            }
                         } else if (ReteIvaTipo1_2 && typeOfRet.indexOf('7') >= 0 && codeCountry == '169') {
                             resultArray[14] = contextValue[6];
                         } else if ((ReteIvaTipo1_2 || autoReteIvaTipo1 || autoReteIvaTipo2) && typeOfRet.indexOf('8') >= 0 && codeCountry != '169') {
                             resultArray[15] = contextValue[6];
                         }

                         //log.error('resultArray', resultArray);

                         if (!(resultArray[12] == 0 && resultArray[13] == 0 && resultArray[14] == 0 && resultArray[15] == 0)) {
                             //key = resultArray[0] + '|' + resultArray[1] + '|' + resultArray[2] + '|' + resultArray[3] + '|' + resultArray[4] + '|' + resultArray[5] + '|' + resultArray[6] + '|' + resultArray[7] + '|' + resultArray[8] + '|' + resultArray[9] + '|' + resultArray[10] + '|' + resultArray[11];
                             key = resultArray[0] + '|' + entityId;

                             if (accountsDetailJson[key] === undefined) {
                                 accountsDetailJson[key] = resultArray;
                             } else {
                                 accountsDetailJson[key][12] += resultArray[12];
                                 accountsDetailJson[key][13] += resultArray[13];
                                 accountsDetailJson[key][14] += resultArray[14];
                                 accountsDetailJson[key][15] += resultArray[15];
                             }
                         }
                     }
                 }
                 return accountsDetailJson;
             }
         }
     } catch (error) {
         log.debug("Error contextValue", contextValue);
         log.debug("FIX ME", error);
     }

 }

 function MaingetCreditNoteAccountsDetail(contextValue) {

     var accountsDetailJson = {};
     var accountsJson = getAccounts(contextValue);
     var accountsIdArray = Object.keys(accountsJson);

     log.error('accountsJson', accountsJson);
     //log.error('accountsIdArray', accountsIdArray);

     if (accountsIdArray && accountsIdArray.length) {

         var newSearch = search.create({

             type: 'account',
             filters: [
                 ['internalid', 'anyof', accountsIdArray],
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
                 }),
                 search.createColumn({
                     name: "custrecord_lmry_co_fields_format_c",
                     join: "custrecord_lmry_co_puc_concept"
                 })
             ]
         });

         var objResult = newSearch.run().getRange(0, 1000);
         var countriesJson = getCountries();
         var resultArray = [],
             key = '';

         if (objResult && objResult.length) {

             for (var j = 0; j < objResult.length; j++) {
                 resultArray = [];
                 var aux_arrinformation = [];

                 columns = objResult[j].columns;

                 // Concepto
                 resultArray[0] = objResult[j].getValue(columns[1]);

                 aux_arrinformation = getInformation(contextValue[0], contextValue[14], contextValue[15]);


                 if (aux_arrinformation.length != 0) {

                     var codeCountry = countriesJson[aux_arrinformation[10]];
                     resultArray = resultArray.concat(aux_arrinformation);

                     var entityType = '',
                         entityId = '';

                     if (contextValue[0] == 'VendBill' || contextValue[0] == 'VendCred') {
                         entityType = 'vendor';
                         entityId = contextValue[14];
                     } else if (contextValue[0] == 'CustCred' || contextValue[0] == 'CustInvc') {
                         entityType = 'customer';
                         entityId = contextValue[15];
                     }

                     // 12, 13 14 y 15
                     resultArray[12] = 0;
                     resultArray[13] = 0;
                     resultArray[14] = 0;
                     resultArray[15] = 0;

                     //log.error('Amount Fte', contextValue[2]);
                     //Rte Fte
                     if (accountsJson[objResult[j].getValue(columns[0])] == 'RteFTE' && contextValue[4] == 'WHT' && objResult[j].getValue(columns[2]).split(',').indexOf('5') >= 0) {
                         resultArray[12] = contextValue[2] || 0;
                     } else if (accountsJson[objResult[j].getValue(columns[0])] == 'RteFTE' && contextValue[4] == 'AutoWHT' && objResult[j].getValue(columns[2]).split(',').indexOf('6') >= 0) {
                        if (contextValue[0] == 'VendBill') {
                            resultArray[13] = contextValue[2] || 0;
                        } else {
                            log.debug('No debe aparecer (se filtra)', contextValue);
                        }
                     }
                     //log.error('Amount Iva', contextValue[1]);
                     //Rte IVa
                     if (accountsJson[objResult[j].getValue(columns[0])] == 'RteIVA' && contextValue[3] == 'WHT' && 
                        objResult[j].getValue(columns[2]).split(',').indexOf('7') >= 0 && codeCountry == '169') {
                            resultArray[14] = contextValue[1] || 0;
                     } else if (accountsJson[objResult[j].getValue(columns[0])] == 'RteIVA' && (contextValue[3] == 'WHT' || contextValue[3] == 'AutoWHT') &&
                        objResult[j].getValue(columns[2]).split(',').indexOf('8') >= 0 && codeCountry != '169' ) {
                            resultArray[15] = contextValue[1] || 0;
                     }

                     log.error('resultArray MAIN- ' + accountsJson[objResult[j].getValue(columns[0])], resultArray);

                     if (!(resultArray[12] == 0 && resultArray[13] == 0 && resultArray[14] == 0 && resultArray[15] == 0)) {
                         //key = resultArray[0] + '|' + resultArray[1] + '|' + resultArray[2] + '|' + resultArray[3] + '|' + resultArray[4] + '|' + resultArray[5] + '|' + resultArray[6] + '|' + resultArray[7] + '|' + resultArray[8] + '|' + resultArray[9] + '|' + resultArray[10] + '|' + resultArray[11];
                         key = resultArray[0] + '|' + entityId;
                         if (accountsDetailJson[key] === undefined) {
                             accountsDetailJson[key] = resultArray;
                         } else {
                             accountsDetailJson[key][12] += resultArray[12];
                             accountsDetailJson[key][13] += resultArray[13];
                             accountsDetailJson[key][14] += resultArray[14];
                             accountsDetailJson[key][15] += resultArray[15];
                         }
                     }
                 }
             }
         }
     }

     return accountsDetailJson;
 }

 function getInformation(transactionType, vendorId, customerId) {

     var type = '',
         internalId = '';

     if (transactionType == 'VendBill' || transactionType == 'VendCred') {
         type = 'vendor';
         internalId = vendorId;
     } else if (transactionType == 'CustCred' || transactionType == 'CustInvc') {
         type = 'customer';
         internalId = customerId;
     } else if (transactionType == 'Journal') {
         if (vendorId != '') {
             type = 'vendor';
             internalId = vendorId;
         } else {
             type = 'customer';
             internalId = customerId;
         }
     }

     //log.error('type', type);
     //log.error('internalId', internalId);

     var newSearch = search.create({
         type: type,
         filters: [
             ['internalid', 'is', internalId],
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
     var auxArray = [];
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

     return auxArray;
 }

 function saveAuxiliaryFile(fileContent, fileNumber) {
     var folderId = objContext.getParameter({
         name: "custscript_lmry_file_cabinet_rg_co"
     });

     if (folderId) {
         var fileName = getFileName(fileNumber);
         var auxiliaryFile = file.create({
             name: fileName,
             fileType: file.Type.PLAINTEXT,
             contents: fileContent,
             encoding: file.Encoding.ISO_8859_1,
             folder: folderId
         });
         var idFile = auxiliaryFile.save(); // id del archivo
         return idFile;
     }
 }

 function getFileName(fileNumber) {
     var userRecord = runtime.getCurrentUser();

     var fileName = "CO_F1001_WITHHOLDINGS_" + userRecord.id + "_" + fileNumber + ".txt";

     return fileName;
 }

 function callMapReducecript(withholdingsFileId) {
     var params = {};
     params["custscript_lmry_co_f1001_v10_mprd_subsi"] = paramSubsidiary;
     params["custscript_lmry_co_f1001_v10_mprd_period"] = paramPeriod;
     params["custscript_lmry_co_f1001_v10_mprd_multi"] = paramMultibook;
     params["custscript_lmry_co_f1001_v10_mprd_rptid"] = paramReportId;
     params["custscript_lmry_co_f1001_v10_mprd_rptvid"] = paramReportVersionId;
     params["custscript_lmry_co_f1001_v10_mprd_logid"] = paramLogId;
     params["custscript_lmry_co_f1001_v10_mprd_concep"] = paramConcept;
     params["custscript_lmry_co_f1001_v10_mprd_whsid"] = withholdingsFileId;

     log.error("parametros enviados", params);
     var taskScript = task.create({
         taskType: task.TaskType.MAP_REDUCE,
         scriptId: 'customscript_lmry_co_f1001_v10_mprd',
         deploymentId: 'customdeploy_lmry_co_f1001_v10_mprd',
         params: params
     });
     taskScript.submit();
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

 function lengthInUtf8Bytes(str) {
     var m = encodeURIComponent(str).match(/%[89ABab]/g);
     return str.length + (m ? m.length : 0);
 }

 function IsPrimary(ParamSub, ParamId) {

     var accountingbookSearchObj = search.create({
         type: "accountingbook",
         filters: [
             ["subsidiary", "anyof", ParamSub],
             "AND", ["internalid", "anyof", ParamId]
         ],
         columns: [
             search.createColumn({ name: "isprimary", label: "Primary" })
         ]
     });

     var objResult = accountingbookSearchObj.run().getRange(0, 1000);
     var columns = objResult[0].columns;
     var val = objResult[0].getValue(columns[0]);

     return val;
 }

 function ObtieneAccxCContri_o_NTax(type, CClass, NTax) {
     if (type == 'EXPENSE') {
         //TIENE contributor class
         if (CClass) {

             var ContributoryClassSearch = search.create({

                 type: "customrecord_lmry_ar_contrib_class",
                 filters: [
                     ["internalid", "anyof", CClass]
                 ],
                 columns: [
                     search.createColumn({
                         name: "formulatext",
                         formula: "{custrecord_lmry_ar_ccl_taxitem.expenseaccount.id}",
                         label: "Formula (Text)"
                     }),
                     search.createColumn({
                         name: "formulatext",
                         formula: "CASE WHEN {custrecord_lmry_ccl_gen_transaction.id} = '1' THEN 'Journal' ELSE CASE WHEN {custrecord_lmry_ccl_gen_transaction.id} = '5' THEN 'WHT by Transaction' ELSE '' END END",
                         label: "Fórmula (text)"
                     }),
                     search.createColumn({
                         name: "formulatext",
                         formula: "{custrecord_lmry_br_ccl_account2.id}",
                         label: "Formula (Text)"
                     }),
                     search.createColumn({
                         name: "formulatext",
                         formula: "{custrecord_lmry_br_ccl_account1.id}",
                         label: "Formula (Text)"
                     })
                 ]

             });
             var objResult = ContributoryClassSearch.run().getRange(0, 1000);
             //TIENE National Tax
         } else if (NTax) {

             var NationalTaxSearch = search.create({

                 type: "customrecord_lmry_national_taxes",
                 filters: [
                     ["internalid", "anyof", NTax]
                 ],
                 columns: [
                     search.createColumn({
                         name: "formulatext",
                         formula: "{custrecord_lmry_ntax_taxitem.expenseaccount.id}",
                         label: "Formula (Text)"
                     }),
                     search.createColumn({
                         name: "formulatext",
                         formula: "CASE WHEN {custrecord_lmry_ntax_gen_transaction.id} = '1' THEN 'Journal' ELSE CASE WHEN {custrecord_lmry_ntax_gen_transaction.id} = '5' THEN 'WHT by Transaction' ELSE '' END END",
                         label: "Fórmula (text)"
                     }),
                     search.createColumn({
                         name: "formulatext",
                         formula: "{custrecord_lmry_ntax_credit_account.id}",
                         label: "Formula (Text)"
                     }),
                     search.createColumn({
                         name: "formulatext",
                         formula: "{custrecord_lmry_ntax_debit_account.id}",
                         label: "Formula (Text)"
                     })
                 ]
             });
             var objResult = NationalTaxSearch.run().getRange(0, 1000);
         }

     } else if (type == 'INCOME') {

         //Es contributor class
         if (CClass) {

             var ContributoryClassSearch = search.create({

                 type: "customrecord_lmry_ar_contrib_class",
                 filters: [
                     ["internalid", "anyof", CClass]
                 ],
                 columns: [
                     search.createColumn({
                         name: "formulatext",
                         formula: "{custrecord_lmry_ar_ccl_taxitem.incomeaccount.id}",
                         label: "Formula (Text)"
                     }),
                     search.createColumn({
                         name: "formulatext",
                         formula: "CASE WHEN {custrecord_lmry_ccl_gen_transaction.id} = '1' THEN 'Journal' ELSE CASE WHEN {custrecord_lmry_ccl_gen_transaction.id} = '5' THEN 'WHT by Transaction' ELSE '' END END",
                         label: "Fórmula (texto)"
                     }),
                     search.createColumn({
                         name: "formulatext",
                         formula: "{custrecord_lmry_br_ccl_account2.id}",
                         label: "Formula (Text)"
                     }),
                     search.createColumn({
                         name: "formulatext",
                         formula: "{custrecord_lmry_br_ccl_account1.id}",
                         label: "Formula (Text)"
                     })
                 ]

             });
             var objResult = ContributoryClassSearch.run().getRange(0, 1000);
             //Es National Tax
         } else if (NTax) {

             var NationalTaxSearch = search.create({

                 type: "customrecord_lmry_national_taxes",
                 filters: [
                     ["internalid", "anyof", NTax]
                 ],
                 columns: [
                     search.createColumn({
                         name: "formulatext",
                         formula: "{custrecord_lmry_ntax_taxitem.incomeaccount.id}",
                         label: "Formula (Text)"
                     }),
                     search.createColumn({
                         name: "formulatext",
                         formula: "CASE WHEN {custrecord_lmry_ntax_gen_transaction.id} = '1' THEN 'Journal' ELSE CASE WHEN {custrecord_lmry_ntax_gen_transaction.id} = '5' THEN 'WHT by Transaction' ELSE '' END END",
                         label: "Fórmula (texto)"
                     }),
                     search.createColumn({
                         name: "formulatext",
                         formula: "{custrecord_lmry_ntax_credit_account.id}",
                         label: "Formula (Text)"
                     }),
                     search.createColumn({
                         name: "formulatext",
                         formula: "{custrecord_lmry_ntax_debit_account.id}",
                         label: "Formula (Text)"
                     })
                 ]
             });
             var objResult = NationalTaxSearch.run().getRange(0, 1000);
             // ['','Journal,'1715','1716']
         }
     }
     return objResult;
 }

 function getAccounts(contextValue) {

     log.error('contextValue', contextValue);

     var account = [];
     var accountJSON = {};
     // RteIVA
     if (Math.abs(contextValue[1]) > 0) {
         if (contextValue[3] == 'WHT') {
             var accIVA = '';
             if ((contextValue[0] == 'VendBill' || contextValue[0] == 'VendCred') && contextValue[6] != '') {
                 //RETEIVA ACCOUNT PURCHASE
                 accIVA = getAccountbyItem(contextValue[6]);
             } else if ((contextValue[0] == 'CustCred' || contextValue[0] == 'CustInvc') && contextValue[5] != '') {
                 //RETEIVA ACCOUNT SALE
                 accIVA = getAccountbyItem(contextValue[5]);
             }
             if (accIVA != '') {
                 accountJSON[accIVA] = 'RteIVA';
             }
         } else if (contextValue[3] == 'AutoWHT') {
             // RETEIVA CREDIT ITEM
             var accountiva1 = contextValue[9];
             // RETEIVA TOPAY ITEM
             var accountiva2 = contextValue[10];
             if (accountiva1 != '')
                 accountJSON[accountiva1] = 'RteIVA';

             if (accountiva2 != '')
                 accountJSON[accountiva2] = 'RteIVA';
         }
         //log.error('accounts Rte Iva', account);
     }
     // RteFTE
     if (Math.abs(contextValue[2]) > 0) {
         if (contextValue[4] == 'WHT') {
             var accFTE = '';
             if ((contextValue[0] == 'VendBill' || contextValue[0] == 'VendCred') && contextValue[8] != '') {
                 //RETEFTE ITEM PURCHASE
                 accFTE = getAccountbyItem(contextValue[8]);
             } else if ((contextValue[0] == 'CustCred' || contextValue[0] == 'CustInvc') && contextValue[7] != '') {
                 //RETEFTE ITEM SALE
                 accFTE = getAccountbyItem(contextValue[7]);
             }
             if (accFTE != '') {
                 accountJSON[accFTE] = 'RteFTE';
             }
         } else if (contextValue[4] == 'AutoWHT') {
             // RETEFTE CREDIT ITEM
             var accountrtf1 = contextValue[11];
             // RETEFTE TOPAY ITEM
             var accountrtf2 = contextValue[12];
             if (accountrtf1 != '')
                 accountJSON[accountrtf1] = 'RteFTE';

             if (accountrtf2 != '')
                 accountJSON[accountrtf2] = 'RteFTE';
         }
         //log.error('accounts Fte', account);
     }
     log.error('accounts', accountJSON);
     return accountJSON;
 }

 function getAccountbyItem(item) {
     var itemSearchObj = search.create({
         type: "item",
         filters: [
             ["internalid", "anyof", item]
         ],
         columns: [
             search.createColumn({
                 name: "formulatext",
                 formula: "{expenseaccount.id}",
                 label: "Formula (Text)"
             })
         ]
     });
     var objResult = itemSearchObj.run().getRange(0, 1000);
     if (objResult && objResult.length) {
         var columns = objResult[0].columns;
         var NewAccount = objResult[0].getValue(columns[0]);

         return NewAccount;
     } else {
         return '';
     }
 }

 function signAmount(type, amount, multi) {
     //'VendBill'  'VendCred'  'CustCred'  'CustInvc'
     var amount;
     if (type == 'CustInvc' || type == 'VendBill') {
         amount = Number(amount) || 0;
     } else if (type == 'CustCred' || type == 'VendCred') {
         amount = Number(amount) * (-1) || 0;
     } else if (type == 'Journal') {
         amount = Math.abs(amount);

     }
     //amount = amount * Number(ObtenerBook(multi));
     return amount;
 }

 function getDatePeriod(_paramPeriod, _featCalendar, _paramSubsidiary) {

    var periodStartDate = "";
    var periodEndDate = "";

    var accountingPeriodSearch = search.create({
        type: "accountingperiod",
        filters: [
            ["isyear", "is", "T"], 
            "AND", ["isquarter", "is", "F"],
            "AND", ["isinactive", "is", "F"],
            "AND", ["formulatext: {periodname}", "contains", _paramPeriod]
        ],
        columns: [
            search.createColumn({name: "startdate", label: "start date"}),
            search.createColumn({name: "enddate", label: "end date"})
        ]
    });

    //Filtro para probar si tiene multiples calendarios                            
    if (_featCalendar == true || _featCalendar == 'T') {
        var varSubsidiary = search.lookupFields({
            type: 'subsidiary',
            id: _paramSubsidiary,
            columns: ['fiscalcalendar']
        });
        var fiscalCalendar = varSubsidiary.fiscalcalendar[0].value;
        // log.debug('featCalendar',featCalendar);

        filterCalendar = search.createFilter({
            name: 'fiscalcalendar',
            operator: search.Operator.IS,
            values: fiscalCalendar
        });
        // Agrega filtro del calendario de la subsidiaria
        accountingPeriodSearch.filters.push(filterCalendar);
    }

    accountingPeriodSearch.run().each(function (result) {
        var columns = result.columns;
        periodStartDate = result.getValue(columns[0]);
        periodEndDate = result.getValue(columns[1]);
        return true;
    });

    return {
        periodStartDate: periodStartDate,
        periodEndDate: periodEndDate
    }
}

function getArrayPeriods(_paramPeriod, _paramSubsidiary) {
    log.debug('_paramPeriod (getArrayPeriods)', _paramPeriod);
    log.debug('_paramSubsidiary (getArrayPeriods)', _paramSubsidiary);

    var featCalendar = runtime.isFeatureInEffect({ feature: "MULTIPLECALENDARS" });
    var period = new Array();

    var licenses = libReport.getLicenses(_paramSubsidiary);
    var featureSpecialPeriod = libReport.getAuthorization(677, licenses);

    //Si tiene activado el Special Period
    if (featureSpecialPeriod == true || featureSpecialPeriod == 'T') {
        log.debug('special Period (getArrayPeriods)', 'special Period (getArrayPeriods)');
        var filterCalendar = new Array();
        //Busqueda general con el paramperiod
        var searchSpecialPeriod = search.create({
            type: 'customrecord_lmry_special_accountperiod',
            filters: [
                ["custrecord_lmry_anio_fisco", "is", _paramPeriod],
                "AND", ["isinactive", "is", "F"],
                "AND", ["custrecord_lmry_adjustment", "is", "F"]
            ],
            columns: ['custrecord_lmry_accounting_period'/* , 'custrecord_lmry_anio_fisco' */]
        });
        //Filtro para probar si tiene multiples calendarios                            
        if (featCalendar == true || featCalendar == 'T') {
            var searchSubsi = search.lookupFields({
                type: 'subsidiary',
                id: _paramSubsidiary,
                columns: ['fiscalcalendar']
            });
            var fiscalCalendar = searchSubsi.fiscalcalendar;
            var jsonFiscalCalendar = JSON.stringify({ id: fiscalCalendar[0].value, nombre: fiscalCalendar[0].text });
            filterCalendar = search.createFilter({
                name: 'custrecord_lmry_calendar',
                operator: search.Operator.IS,
                values: jsonFiscalCalendar
            });
            // Agrega filtro del calendario de la subsidiaria
            searchSpecialPeriod.filters.push(filterCalendar);
        }
        // Ejecutando la busqueda
        var varResult = searchSpecialPeriod.run();
        var varSpecialPeriodRpt = varResult.getRange({
            start: 0,
            end: 1000
        });
        if (varSpecialPeriodRpt == null || varSpecialPeriodRpt.length == 0) {
            log.debug('NO DATA', 'No hay periodos para ese año seleccionado en la configuración del record LatamReady - Special Accounting Period');
            return false;
        } else {
            for (var i = 0; i < varSpecialPeriodRpt.length; i++) {
                period[i] = new Array();
                period[i] = varSpecialPeriodRpt[i].getValue('custrecord_lmry_accounting_period');
            }
            //log.debug('special accounting period', period);
        }

    } else {
        log.debug('accounting Period (getArrayPeriods)', 'accounting Period (getArrayPeriods)');
        var filterCalendar = new Array();

        var objDatePeriod = getDatePeriod(_paramPeriod, featCalendar, _paramSubsidiary);

        var accountingperiodObj = search.create({
            type: "accountingperiod",
            filters: [
                ["isquarter", "is", "F"],
                "AND", ["isyear", "is", "F"],
                "AND", ["isinactive", "is", "F"],
                "AND", ["isadjust", "is", "F"],
                "AND", ["startdate", "onorafter", objDatePeriod.periodStartDate],
                "AND", ["enddate", "onorbefore", objDatePeriod.periodEndDate]
            ],
            columns: [
                /* search.createColumn({
                    name: "periodname",
                    sort: search.Sort.ASC,
                    label: "Name"
                }), */
                search.createColumn({ name: "internalid", label: "Internal ID" })/* ,
                search.createColumn({ name: "startdate", label: "Start Date" }),
                search.createColumn({ name: "enddate", label: "End Date" }) */
            ]
        });

        //Filtro para probar si tiene multiples calendarios                            
        if (featCalendar == true || featCalendar == 'T') {
            var varSubsidiary = search.lookupFields({
                type: 'subsidiary',
                id: _paramSubsidiary,
                columns: ['fiscalcalendar']
            });
            var fiscalCalendar = varSubsidiary.fiscalcalendar[0].value;
            // log.debug('featCalendar',featCalendar);

            filterCalendar = search.createFilter({
                name: 'fiscalcalendar',
                operator: search.Operator.IS,
                values: fiscalCalendar
            });
            // Agrega filtro del calendario de la subsidiaria
            accountingperiodObj.filters.push(filterCalendar);
        }

        // Ejecutando la busqueda
        var varResult = accountingperiodObj.run();
        var AccountingPeriodRpt = varResult.getRange({
            start: 0,
            end: 1000
        });
        if (AccountingPeriodRpt == null || AccountingPeriodRpt.length == 0) {
            log.debug('NO DATA', 'No hay periodos para ese año seleccionado en la configuración del record LatamReady - Accounting Special Period');
            return false;
        } else {
            for (var i = 0; i < AccountingPeriodRpt.length; i++) {
                period[i] = new Array();
                period[i] = AccountingPeriodRpt[i].getValue('internalid');
            }
            //log.debug('accounting period', period);
        }

    }
    log.debug('period (getArrayPeriods)', period);
    return period;
}

function getFormulPeriodsFilters(_arrayPeriods) {
    var formula = '0';
    if (_arrayPeriods.length > 0) {
        var periodos = _arrayPeriods.map(function e(p) {
            return p // id de Accounting Period
        });
        /* if (hasMultibookFeature) {
            formula = "CASE WHEN {transaction.postingperiod.id} in (" + periodos.join();
        } else { */
            formula = "CASE WHEN {postingperiod.id} in (" + periodos.join();
        /* } */
        formula += ") THEN 1 ELSE 0 END";
        log.debug("formula (getFormulPeriodsFilters)", formula);
    }

    return formula;
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
            'custrecord_lmry__mx_contrycode' // CODE
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

 return {
     getInputData: getInputData,
     map: map,
     reduce: reduce,
     summarize: summarize
 };

});