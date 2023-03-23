/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||  This script for Report - Colombia                           ||
||                                                              ||
||  File Name: LMRY_CO_CertificadoRetencion_MPRD_V2.0.js        ||
||                                                              ||
||  Version  Date          Author          Remarks              ||
||  2.0      Feb 02 2022   Alexandra SF.   Use Script 2.0       ||
 \= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.0
 * @NScriptType MapReduceScript
 * @NModuleScope Public
 */
define(['N/search',
        'N/suiteAppInfo',
        "N/format",
        'N/config',
        'N/record',
        'N/file',
        'N/log',
        'N/task',
        'N/runtime',
        './CO_Library/LMRY_CO_AccessData_LBRY_V2.0',
        './CO_Library/LMRY_CO_GenerateJson_LBRY_V2.0',
        './CO_Library/LMRY_CO_Cache_LBRY_V2.0',
        "/SuiteBundles/Bundle 37714/Latam_Library/LMRY_LibraryReport_LBRY_V2.js",
        "./CO_Library/LMRY_CO_Library_LBRY_V2.0.js"
    ],

    function(search, suiteAppInfo, format, config, record, file, log, task, runtime, AccessData, GenerateJson, DataCache, libreriaReport, libColombia) {

        /**
         * Input Data for processing
         *
         * @return Array,Object,Search,File
         *
         * @since 2016.1
         */

        var objContext = runtime.getCurrentScript();
        var language = runtime.getCurrentScript().getParameter("LANGUAGE").substring(0, 2);
        //PARAMETROS
        var param_Json = {};

        var legal_type_transaction = null;
        var legal_template_transaction = null;
        var legal_template_extension = null;
        var legal_other_record = [];
        var transactionFile = null;
        var nombre_reporte = '';
        var retType = '';
        var autoRetType = '';

        var featureSTXT = null;
        var arrTransactions = new Array;
        var licenses;
        var featureCabecera;
        var featureLineas;

        // IDS DE LA BUSQUEDA POR TOTALES
        var filtroRetencion;
        var filtroRetencionName;
        var filtroRetencionAmount;
        var columnaCodWht;
        var columnaWhtAmount;
        var columnaTasa;

        function getInputData() {
            log.debug("entro a getInputData");

            ObtenerParametros();
            DataCache.setCache(legal_other_record);

            if (featureCabecera) {
                log.debug("entro a Busqueda de cabecera");
                arrTransactions = getTransactionMainByIds();
            } else {
                log.debug("entro a Busqueda de lineas");
                arrTransactions = getTransactionLineByIds();
            }
            log.debug('arrTransactions', arrTransactions);
            return arrTransactions;

        }

        function map(context) {
            log.debug("entro a map");
            try {
                ObtenerParametros();
                var transactionResponse = {};
                var transactionId = JSON.parse(context.value);
                log.debug('transactionId', transactionId);

                var dataCache = DataCache.getDataCache();

                if (featureCabecera) {
                    var search_result = getMainInformation(transactionId);
                } else {
                    var search_result = getLineInformation(transactionId);
                }
                log.debug('search_result', search_result);

                // JSON ARRAY
                if (search_result[6] != '' && search_result[6] != null) {
                    if (featureCabecera || !featureSTXT) {
                        var jsonTaxArray = search_result[6];
                    } else {
                        var jsonTaxArray = JSON.parse(search_result[6]);
                    }
                } else {
                    var jsonTaxArray = [];
                }

                log.debug('jsonTaxArray', jsonTaxArray);


                if (jsonTaxArray.length != 0) {

                    transactionResponse['transaction_id'] = transactionId;

                    //Type
                    if (search_result[2] != '' && search_result[2] != null) {
                        var transactionType = search_result[2];
                    } else {
                        var transactionType = '';
                    }
                    transactionResponse['transaction_type'] = search_result[2];

                    //# No. Factura
                    if (search_result[3] != '' && search_result[3] != null) {
                        var documentNumber = search_result[3];
                    } else {
                        var documentNumber = '';
                    }
                    transactionResponse['transaction_no_factura'] = documentNumber;

                    var taxArrayByRetType = [];

                    for (var i = 0; i < jsonTaxArray.length; i++) {

                        if (jsonTaxArray[i]['subtype']['text'] == retType || jsonTaxArray[i]['subtype']['text'] == autoRetType) {

                            taxArrayByRetType.push(jsonTaxArray[i]);

                        }
                    }
                    transactionResponse['transaction_json'] = taxArrayByRetType;

                    if (search_result[7] != '' && search_result[7] != null && (param_Json.id_Tipo_de_retencion == 1) ) {
                        transactionResponse['municipality'] = getNameMumnicipality(search_result[7]);
                    } else {
                        transactionResponse['municipality'] = '';
                    }

                    context.write({
                        key: transactionId,
                        value: transactionResponse
                    });

                }

            } catch (e) {
                log.error('[ Map Error ]', e);
            }

        }

        function reduce(context) {

        }

        function summarize(context) {
            log.debug("entro a summarize");


            ObtenerParametros();

            //************* OBTENER DATOS DE LA MUNICIPALIDAD ******

            if (param_Json.id_Tipo_de_retencion == 1) {
                var municipality = getMunicipalityByVendorICA(param_Json.id_Vendor) || getMunicipalityBySubsidiary(param_Json.id_subsidiary);
                municipality = municipality || 'BOGOTA';
            } else {
                var municipality = getMunicipalityByVendorOthers(param_Json.id_Vendor) || 'BOGOTA';
            }

            //*************transaction******
            var numTransactions = 0;
            var transactionJSON = {};
            var dataCache = DataCache.getDataCache();

            transactionJSON["parametros"] = param_Json;
            transactionJSON["transaction"] = {};
            transactionJSON["muni_by_vendor_o_subsi"] = municipality;

            context.output.iterator().each(function(key, value) {

                value = JSON.parse(value);
                numTransactions++;
                transactionJSON["transaction"][value.transaction_id] = value;
                return true;

            });

            for (var key in dataCache) {
                if (key.indexOf('customrecord_') > -1) {
                    transactionJSON[key] = AccessData.getCustomRecord(key, dataCache[key]);
                }
            }

            var FolderId = objContext.getParameter({
                name: 'custscript_lmry_file_cabinet_rg_co_stx'
            });
            log.error('[FolderId]', FolderId);

            var transactionFile = file.create({
                name: 'transactionCERT_RET.json',
                fileType: file.Type.JSON,
                contents: JSON.stringify(transactionJSON),
                folder: FolderId //18966
            }).save();

            DataCache.deleteCache();

            if (numTransactions > 0) {
                log.debug("LLAMAR SCHDL");
                DataCache.setCacheByKey("certificado_de_retencion", transactionJSON)
                LlamarSchedule();
            } else {
                noData();
            }
            return true;

        }

        
        function getMunicipalityByVendorICA(idvendor) {

            var municipalidad = '';

            if (idvendor != '' && idvendor != null) {

                var vendorTemp = search.lookupFields({
                    type: search.Type.VENDOR,
                    id: idvendor,
                    columns: ['custentity_lmry_municipality']
                });

                if (vendorTemp.custentity_lmry_municipality.length != 0) {
                    var municipality_id = vendorTemp.custentity_lmry_municipality[0].value;
                }

                municipalidad = getNameMumnicipality(municipality_id);
            }
            log.debug('municipality by vendor:', municipalidad);
            return municipalidad;

        }

        function getMunicipalityByVendorOthers(idvendor) {
            var municipalidad = '';

            if (idvendor != '' && idvendor != null) {

                var vendorSearchObj = search.create({
                    type: "vendor",
                    filters: [
                        ["internalid", "anyof", idvendor]
                    ],
                    columns: [
                        search.createColumn({
                            name: "custrecord_lmry_addr_city",
                            join: "Address",
                            label: "Latam - City"
                        })
                    ]
                });
                var objResult = vendorSearchObj.run().getRange(0, 1000);
                if (objResult && objResult.length) {
                    var columns = objResult[0].columns;
                    municipalidad = objResult[0].getText(columns[0]);
                }

            }
            log.debug('municipality by vendor:', municipalidad);
            return municipalidad;
        }

        function getMunicipalityBySubsidiary(paramsubsidi) {

            var municipalidad = '';

            if (paramsubsidi != '' && paramsubsidi != null) {
                var municipality_id_Temp = search.lookupFields({
                    type: search.Type.SUBSIDIARY,
                    id: paramsubsidi,
                    columns: ['custrecord_lmry_municipality_sub']
                });

                if (municipality_id_Temp.custrecord_lmry_municipality_sub.length != 0) {
                    var municipality_id = municipality_id_Temp.custrecord_lmry_municipality_sub[0].value;
                }
                if (municipality_id != '' && municipality_id != null) {
                    municipalidad = getNameMumnicipality(municipality_id);
                }
            }
            log.debug('municipality by Subsidiaria:', municipalidad);
            return municipalidad;
        }

        function getNameMumnicipality(municipality_id) {

            var municipalidad = '';

            if (municipality_id != '' && municipality_id != null) {

                var municipality_Temp = search.lookupFields({
                    type: 'customrecord_lmry_co_entitymunicipality',
                    id: municipality_id,
                    columns: ['custrecord_lmry_co_municcode']
                });

                var code_municipality = municipality_Temp.custrecord_lmry_co_municcode;

                var searchCity = search.create({
                    type: "customrecord_lmry_city",
                    filters: [
                        ["custrecord_lmry_city_country", "anyof", "48"],
                        "AND", ["custrecord_lmry_city_id", "is", code_municipality]
                    ],
                    columns: [
                        search.createColumn({
                            name: "name",
                        })
                    ]
                });

                var resultObj = searchCity.run();
                var searchResultArray = resultObj.getRange(0, 1000);

                if (searchResultArray != null && searchResultArray.length != 0) {
                    municipalidad = searchResultArray[0].getValue("name");
                    if (municipalidad != '' && municipalidad != null) {
                        municipalidad = municipalidad.replace('BOGOTA BOGOTA, D.C.', 'BOGOTA');
                    }
                }
            }

            return municipalidad;
        }

        function LlamarSchedule() {
            var params = {};

            params['custscript_lmry_co_cert_retenc_globales'] = objContext.getParameter('custscript_lmry_co_certret_globales');
            params['custscript_lmry_co_cert_retenc_idfield'] = transactionFile;


            var RedirecSchdl = task.create({
                taskType: task.TaskType.SCHEDULED_SCRIPT,
                scriptId: 'customscript_lmry_co_cert_ret_schdl',
                deploymentId: 'customdeploy_lmry_co_cert_ret_schdl',
                params: params
            });
            RedirecSchdl.submit();

        }

        function noData() {
            log.debug("NO DATA");
            libColombia.loadNoData(param_Json.id_rpt_generator_log);
        }

        function saveFile(render_outpuFile) {
            var FolderId = objContext.getParameter({
                name: 'custscript_lmry_file_cabinet_rg_co_stx'
            });
            var nameFile = nombre_reporte;
            var urlfile = libreriaReport.setFile(FolderId, render_outpuFile, nameFile);

            log.debug('param_Json', param_Json.id_rpt_generator_log);
            libColombia.load(param_Json.id_rpt_generator_log, nameFile, urlfile);

        }

        function ObtenerParametros() {

            /*Bundle Suite Tax Engine*/
            var isBundleInstalled = suiteAppInfo.isBundleInstalled({
                bundleId: 237702
            });

            if (isBundleInstalled) {
                featureSTXT = true;
            } else {
                featureSTXT = false;
            }

            var nameDIAN = objContext.getParameter({
                name: 'custscript_lmry_co_dian_name'
            });
            if (nameDIAN == null || nameDIAN == "- None -" || nameDIAN == "") {
                nameDIAN = ' ';
            }

            var param_Globales = objContext.getParameter('custscript_lmry_co_certRet_globales'); // || {};
            var param_Others = objContext.getParameter('custscript_lmry_co_certRet_others'); // || {};

            param_Globales = JSON.parse(param_Globales);

            param_Globales = {
                id_rpt_generator_log: param_Globales.id_RecordLog,
                id_rpt_feature: param_Globales.id_Report,
                id_legaltemplate: param_Globales.id_LegalTemplate,
                id_subsidiary: param_Globales.id_Subsidiary,
                id_multibook: param_Globales.id_Multibook,
                id_period: param_Globales.id_Period,
                id_legalledger: param_Globales.id_LegalLedger,
                id_lenguaje: param_Globales.id_Language
            }

            param_Others = JSON.parse(param_Others);
            param_Json = libreriaReport.mergeObject(param_Json, param_Globales);

            var fecha = new Date();
            var mes = fecha.getMonth() + 1;
            if ((mes + '').length == 1) {
                mes = '0' + mes;
            }

            param_Others = {
                id_Vendor: param_Others.id_vendor,
                id_Tipo_de_retencion: param_Others.id_tipo_de_retencion,
                id_Fecha_ini: param_Others.id_fecha_ini,
                id_Fecha_fin: param_Others.id_fecha_fin,
                period_anio: fecha.getFullYear(),
                period_mes: mes,
                namedian: nameDIAN
            }

            param_Json = libreriaReport.mergeObject(param_Json, param_Others);

            //LatamReady - Legal Template
            var legal_template_temp = search.lookupFields({
                type: 'customrecord_lmry_legaltemplate',
                id: param_Json.id_legaltemplate,
                columns: ['custrecord_lmry_templates', 'custrecord_lmry_templateextension', ]
            });
            legal_template_transaction = legal_template_temp.custrecord_lmry_templates;
            legal_template_extension = legal_template_temp.custrecord_lmry_templateextension;

            nombre_reporte = nameFile(legal_template_extension, param_Json.id_rpt_feature);

            //LatamReady - Legal Ledger
            var legal_ledger_temp = search.lookupFields({
                type: 'customrecord_lmry_legalreport',
                id: param_Json.id_legalledger,
                columns: ['custrecord_lmry_legaltypetrans', 'custrecord_lmry_legalotherrecords']
            });

            legal_type_transaction = legal_ledger_temp.custrecord_lmry_legaltypetrans;
            legal_other_record = legal_ledger_temp.custrecord_lmry_legalotherrecords;

            var auxiliar = [];
            if (legal_other_record != '') {
                var type = JSON.parse(legal_other_record);
                for (var i = 0; i < type.length; i++) {
                    var auxiliar_type = JSON.stringify(type[i]);
                    var unidades = JSON.parse(auxiliar_type);
                    auxiliar[i] = unidades.Code;
                }
            }
            legal_other_record = auxiliar;

            //TODO: Obtiene el feature de retenciones por linea o cabecera

            licenses = libreriaReport.getLicenses(param_Json.id_subsidiary);
            featureCabecera = libreriaReport.getAuthorization(27, licenses);
            featureLineas = libreriaReport.getAuthorization(340, licenses);

            //TODO: Se escoge los ids para los filtros de la busqueda de retenciones por totales

            var tipoRetencion = String(param_Json.id_Tipo_de_retencion);

            switch (tipoRetencion) {
                case '1':
                    retType = 'ReteICA';
                    autoRetType = 'Auto ReteICA';
                    filtroRetencion = "formulatext: {custbody_lmry_co_reteica}";
                    filtroRetencionName = "formulatext: CASE WHEN {custbody_lmry_co_reteica.name} = 'ReteICA 0%' THEN 1 ELSE 0 END";
                    filtroRetencionAmount = "formulatext: CASE WHEN({custbody_lmry_co_reteica_amount}>0) THEN 1 ELSE 0 END";
                    columnaCodWht = "{vendor.custentity_lmry_co_reteica}";
                    columnaWhtAmount = "{custbody_lmry_co_reteica_amount}";
                    columnaTasa = "{custbody_lmry_co_reteica.custrecord_lmry_wht_codedesc}";
                    break;
                case '2':
                    retType = 'ReteFTE';
                    autoRetType = 'Auto ReteFTE';
                    filtroRetencion = "formulatext: {custbody_lmry_co_retefte}";
                    filtroRetencionName = "formulatext: CASE WHEN {custbody_lmry_co_retefte.name} = 'ReteFte 0%' THEN 1 ELSE 0 END";
                    filtroRetencionAmount = "formulatext: CASE WHEN({custbody_lmry_co_retefte_amount}>0) THEN 1 ELSE 0 END";
                    columnaCodWht = "{vendor.custentity_lmry_co_retefte}";
                    columnaWhtAmount = "{custbody_lmry_co_retefte_amount}";
                    columnaTasa = "{custbody_lmry_co_retefte.custrecord_lmry_wht_codedesc}";
                    break;
                case '3':
                    retType = 'ReteIVA';
                    autoRetType = 'Auto ReteIVA';
                    filtroRetencion = "formulatext: {custbody_lmry_co_reteiva}";
                    filtroRetencionName = "formulatext: CASE WHEN {custbody_lmry_co_reteiva.name} = 'ReteIVA 0%' THEN 1 ELSE 0 END";
                    filtroRetencionAmount = "formulatext: CASE WHEN({custbody_lmry_co_reteiva_amount}>0) THEN 1 ELSE 0 END";
                    columnaCodWht = "{vendor.custentity_lmry_co_reteiva}";
                    columnaWhtAmount = "{custbody_lmry_co_reteiva_amount}";
                    columnaTasa = "{custbody_lmry_co_reteiva.custrecord_lmry_wht_codedesc}";
                    break;
            }

        }

        function nameFile(id_extension, id_reporte) {

            var name = '';
            if (param_Json.id_subsidiary != '') {
                if (featureSTXT) {
                    var legal_ledger_temp = search.lookupFields({
                        type: 'subsidiary',
                        id: param_Json.id_subsidiary,
                        columns: ['custrecord_lmry_taxregnumber']
                    });
                    var companyruc = legal_ledger_temp.custrecord_lmry_taxregnumber;
                } else {
                    var legal_ledger_temp = search.lookupFields({
                        type: 'subsidiary',
                        id: param_Json.id_subsidiary,
                        columns: ['taxidnum']
                    });
                    var companyruc = legal_ledger_temp.taxidnum;
                }

            } else {
                if (featureSTXT) {
                    var pageConfig = config.load({
                        type: config.Type.COMPANY_INFORMATION
                    });
                    var companyruc = pageConfig.getValue('custrecord_lmry_taxregnumber');

                } else {
                    var pageConfig = config.load({
                        type: config.Type.COMPANY_INFORMATION
                    });
                    var companyruc = pageConfig.getValue('taxidnum');

                }

            }

            name = 'COCertificado_' + companyruc + '_' + param_Json.period_mes + '_' + param_Json.period_anio + '_' + param_Json.id_subsidiary + '.' + id_extension;


            return name;
        }

        function getTransactionLineByIds() {

            var formulaBrTransactionType = "CASE WHEN {custrecord_lmry_br_transaction.custrecord_lmry_br_type} IN ('" + retType + "', '" + autoRetType + "') THEN '1' ELSE '0' END";

            var transactionSearchObj = search.create({
                type: "transaction",
                filters: [
                    ["posting", "is", "T"],
                    "AND", ["type", "anyof", "VendBill", "VendCred"],
                    "AND", ["mainline", "is", "T"],
                    "AND", ["voided", "is", "F"],
                    "AND", ["formulatext: CASE WHEN {customform} = 'Latam WHT - Vendor Bill' THEN 1 ELSE CASE WHEN {customform} = 'Latam WHT - Vendor Credit' THEN 2 ELSE 0 END END", "is", "0"],
                ],
                columns: [
                    search.createColumn({
                        name: "internalid",
                        summary: "GROUP",
                        label: "Internal ID"
                    })
                ]
            });

            if (featureSTXT) {

                var JsonTaxFilter = search.createFilter({
                    name: "custrecord_lmry_ste_wht_transaction",
                    join: "custrecord_lmry_ste_related_transaction",
                    operator: search.Operator.ISNOTEMPTY
                });
                transactionSearchObj.filters.push(JsonTaxFilter);

            } else {

                var brTransacionFilter = search.createFilter({
                    name: "formulatext",
                    formula: formulaBrTransactionType,
                    operator: search.Operator.IS,
                    values: ['1']
                });
                transactionSearchObj.filters.push(brTransacionFilter);

            }

            if (param_Json.id_subsidiary != '') {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [param_Json.id_subsidiary]
                });
                transactionSearchObj.filters.push(subsidiaryFilter);
            }

            var periodFilterIni = search.createFilter({
                name: 'trandate',
                operator: search.Operator.ONORAFTER,
                values: [param_Json.id_Fecha_ini]
            });
            transactionSearchObj.filters.push(periodFilterIni);

            var periodFilterFin = search.createFilter({
                name: 'trandate',
                operator: search.Operator.ONORBEFORE,
                values: [param_Json.id_Fecha_fin]
            });
            transactionSearchObj.filters.push(periodFilterFin);

            var entityFilter = search.createFilter({
                name: 'name',
                operator: search.Operator.IS,
                values: [param_Json.id_Vendor]
            });
            transactionSearchObj.filters.push(entityFilter);

            if (param_Json.id_multibook != '') {
                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [param_Json.id_multibook]
                });
                transactionSearchObj.filters.push(multibookFilter);

            }

            var myPageData = transactionSearchObj.runPaged({
                pageSize: 1000
            });

            var page, idTransaction;
            var transactionsArray = [];

            myPageData.pageRanges.forEach(function(pageRange) {

                page = myPageData.fetch({
                    index: pageRange.index
                });
                page.data.forEach(function(result) {
                    idTransaction = '';
                    // 0. Internal ID
                    if (result.getValue(result.columns[0]) != '- None -') {
                        idTransaction = result.getValue(result.columns[0]);
                    } else {
                        idTransaction = '';
                    }
                    transactionsArray.push(idTransaction);
                });
            });

            return transactionsArray;

        }

        function getTransactionMainByIds() {

            var transactionSearchObj = search.create({
                type: "transaction",
                filters: [
                    ["posting", "is", "T"],
                    "AND", ["type", "anyof", "VendBill", "VendCred"],
                    "AND", ["voided", "is", "F"],
                    "AND", ["taxline", "is", "F"],
                    "AND", ["mainline", "is", "F"],
                    "AND", [filtroRetencion, "isnotempty", ""],
                    "AND", ["formulatext: {custbody_lmry_apply_wht_code}", "is", "T"],
                    "AND", [filtroRetencionName, "is", "0"],
                    "AND", [filtroRetencionAmount, "is", "1"],
                    "AND", ["formulatext: CASE WHEN {customform} = 'Latam WHT - Vendor Bill' THEN 1 ELSE CASE WHEN {customform} = 'Latam WHT - Vendor Credit' THEN 2 ELSE 0 END END", "is", "0"],
                    "AND", ["formulanumeric: {vendor.internalid}", "equalto", param_Json.id_Vendor]
                ],
                columns: [
                    search.createColumn({
                        name: "internalid",
                        summary: "GROUP",
                        label: "0.Internal ID"
                    }),
                ]
            });

            if (param_Json.id_subsidiary != '') {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [param_Json.id_subsidiary]
                });
                transactionSearchObj.filters.push(subsidiaryFilter);
            }

            var periodFilterIni = search.createFilter({
                name: 'trandate',
                operator: search.Operator.ONORAFTER,
                values: [param_Json.id_Fecha_ini]
            });
            transactionSearchObj.filters.push(periodFilterIni);

            var periodFilterFin = search.createFilter({
                name: 'trandate',
                operator: search.Operator.ONORBEFORE,
                values: [param_Json.id_Fecha_fin]
            });
            transactionSearchObj.filters.push(periodFilterFin);


            if (param_Json.id_multibook != '') {
                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [param_Json.id_multibook]
                });
                transactionSearchObj.filters.push(multibookFilter);

            }

            var myPageData = transactionSearchObj.runPaged({
                pageSize: 1000
            });

            var page, idTransaction;
            var transactionsArray = [];

            myPageData.pageRanges.forEach(function(pageRange) {

                page = myPageData.fetch({
                    index: pageRange.index
                });
                page.data.forEach(function(result) {
                    idTransaction = '';
                    // 0. Internal ID
                    if (result.getValue(result.columns[0]) != '- None -') {
                        idTransaction = result.getValue(result.columns[0]);
                    } else {
                        idTransaction = '';
                    }
                    transactionsArray.push(idTransaction);
                });
            });

            return transactionsArray;

        }

        function getLineInformation(id) {
            var auxArray;
            if (featureSTXT) {

                var transactionSearchObj = search.create({
                    type: "transaction",
                    filters: [
                        ["internalid", "anyof", id],
                        "AND", ["mainline", "is", "T"]
                    ],
                    columns: [
                        search.createColumn({ name: "internalid", label: "0. Internal ID" }),
                        search.createColumn({ name: "entity", label: "1. Name" }),
                        search.createColumn({ name: "type", label: "2. Type" }),
                        search.createColumn({ name: "tranid", label: "3. Document Number" }),
                        search.createColumn({ name: "memo", label: "4. Memo" }),
                        search.createColumn({ name: "trandate", label: "5. Date" }),
                        search.createColumn({
                            name: "custrecord_lmry_ste_wht_transaction",
                            join: "CUSTRECORD_LMRY_STE_RELATED_TRANSACTION",
                            label: "6. JSON"
                        })
                    ]
                })

                if (param_Json.id_Tipo_de_retencion == 1) {
                    //7.- Municipalidad
                    var municipTransaction = search.createColumn({
                        name: "internalid",
                        join: "CUSTBODY_LMRY_MUNICIPALITY",
                        summary: "GROUP",
                        label: "7. Municipality"
                    });
                    transactionSearchObj.columns.push(municipTransaction);
                }

                var myPageData = transactionSearchObj.runPaged({
                    pageSize: 1000
                });

                var page;

                myPageData.pageRanges.forEach(function(pageRange) {

                    page = myPageData.fetch({
                        index: pageRange.index
                    });
                    page.data.forEach(function(result) {
                        auxArray = [];
                        // 0. Internal ID
                        if (result.getValue(result.columns[0]) != '- None -' || result.getValue(result.colums[0]) != null) {
                            auxArray[0] = result.getValue(result.columns[0]);
                        } else {
                            auxArray[0] = '';
                        }

                        // 1. Name
                        if (result.getValue(result.columns[1]) != '- None -' || result.getValue(result.colums[1]) != null) {
                            auxArray[1] = result.getValue(result.columns[1]);
                        } else {
                            auxArray[1] = '';
                        }

                        // 2. Type
                        if (result.getValue(result.columns[2]) != '- None -' || result.getValue(result.colums[2]) != null) {
                            auxArray[2] = result.getValue(result.columns[2]);
                        } else {
                            auxArray[2] = '';
                        }

                        // 3. Document Number
                        if (result.getValue(result.columns[3]) != '- None -' || result.getValue(result.colums[3]) != null) {
                            auxArray[3] = result.getValue(result.columns[3]);
                        } else {
                            auxArray[3] = '';
                        }

                        // 4. Memo
                        if (result.getValue(result.columns[4]) != '- None -' || result.getValue(result.colums[4]) != null) {
                            auxArray[4] = result.getValue(result.columns[4]);
                        } else {
                            auxArray[4] = '';
                        }

                        // 5. Date
                        if (result.getValue(result.columns[5]) != '- None -' || result.getValue(result.colums[5]) != null) {
                            auxArray[5] = result.getValue(result.columns[5]);
                        } else {
                            auxArray[5] = '';
                        }

                        // 6. JSON
                        if (result.getValue(result.columns[6]) != '- None -' || result.getValue(result.colums[6]) != null) {
                            auxArray[6] = result.getValue(result.columns[6]);
                        } else {
                            auxArray[6] = '';
                        }

                        // 7. MUNICIPALIDAD
                        if (result.getValue(result.columns[7]) != '- None -' || result.getValue(result.colums[7]) != null) {
                            auxArray[6] = result.getValue(result.columns[7]);
                        } else {
                            auxArray[6] = '';
                        }

                    });
                });
            } else {

                var intDMinReg = 0;
                var intDMaxReg = 1000;
                var DbolStop = false;
                var auxArray;
                var tasa, netAmount, retAmount, concepto;
                var auxArrJson = [];
                var formulaBrTransactionType = "CASE WHEN {custrecord_lmry_br_transaction.custrecord_lmry_br_type} IN ('" + retType + "', '" + autoRetType + "') THEN '1' ELSE '0' END";

                var transactionSearchObj = search.create({
                    type: "transaction",
                    filters: [
                        ["mainline", "is", "T"],
                        "AND", ["internalid", "anyof", id]
                    ],
                    columns: [
                        search.createColumn({
                            name: "internalid",
                            label: "0. Internal ID"
                        }),
                        search.createColumn({
                            name: "formulatext",
                            formula: "{custrecord_lmry_br_transaction.custrecord_lmry_tax_description}",
                            label: "1. Código WHT"
                        }),
                        search.createColumn({ name: "type", label: "2. Tipo de Transaccion" }),
                        search.createColumn({ name: "tranid", label: "3. Numero de Factura" }),
                        search.createColumn({
                            name: "formulacurrency",
                            formula: "{custrecord_lmry_br_transaction.custrecord_lmry_base_amount_local_currc}",
                            label: "4. Base imponible"
                        }),
                        search.createColumn({
                            name: "formulacurrency",
                            formula: "{custrecord_lmry_br_transaction.custrecord_lmry_amount_local_currency}",
                            label: "5. Retencion"
                        }),
                        search.createColumn({
                            name: "formulanumeric",
                            formula: "{custrecord_lmry_br_transaction.custrecord_lmry_br_percent} * 10000",
                            label: "6. TASA"
                        })
                    ]
                });

                var brTransacionFilter = search.createFilter({
                    name: "formulatext",
                    formula: formulaBrTransactionType,
                    operator: search.Operator.IS,
                    values: ['1']
                });
                transactionSearchObj.filters.push(brTransacionFilter);

                if (param_Json.id_Tipo_de_retencion == 1) {
                    //7.- Municipalidad
                    var municipTransaction = search.createColumn({
                        name: "internalid",
                        join: "CUSTBODY_LMRY_MUNICIPALITY",
                        summary: "GROUP",
                        label: "7. Municipality"
                    });
                    transactionSearchObj.columns.push(municipTransaction);
                }

                var searchresult = transactionSearchObj.run();

                while (!DbolStop) {
                    var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

                    if (objResult != null) {
                        var intLength = objResult.length;
                        if (intLength != 1000) {
                            DbolStop = true;
                        }

                        for (var i = 0; i < intLength; i++) {
                            var columns = objResult[i].columns;
                            tasa = '', netAmount = '', retAmount = '', concepto = '';
                            auxArray = [];

                            //0. Internal ID
                            if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -') {
                                auxArray[0] = objResult[i].getValue(columns[0]);
                            } else {
                                auxArray[0] = '';
                            }

                            auxArray[1] = '';

                            // 1. Código WHT
                            if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -') {
                                concepto = objResult[i].getValue(columns[1]);
                            } else {
                                concepto = '';
                            }

                            // 2. Type
                            if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -') {
                                auxArray[2] = objResult[i].getValue(columns[2]);
                            } else {
                                auxArray[2] = '';
                            }

                            // 3. No Factura
                            if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -') {
                                auxArray[3] = objResult[i].getValue(columns[3]);
                            } else {
                                auxArray[3] = '';
                            }

                            auxArray[4] = '';
                            auxArray[5] = '';

                            // 4. NET AMOUNT
                            if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -') {
                                netAmount = String(Math.abs(objResult[i].getValue(columns[4])));
                            } else {
                                netAmount = '0.00';
                            }

                            // 5. RETENCION
                            if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -') {
                                if (objResult[i].getValue(columns[2]) == "VendCred") {
                                    retAmount = String(Number(objResult[i].getValue(columns[5])) * (-1));
                                } else {
                                    retAmount = objResult[i].getValue(columns[5]);
                                }
                            } else {
                                retAmount = '0.00';
                            }

                            // 6. BASE IMPONIBLE
                            if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -') {
                                tasa = Number(objResult[i].getValue(columns[6])).toFixed();
                            } else {
                                tasa = '0';
                            }

                            // 6. JSON
                            auxArrJson.push({
                                subtype: {
                                    text: retType
                                },
                                lc_baseamount: netAmount,
                                lc_whtamount: retAmount,
                                whtrate: tasa,
                                description: concepto
                            });

                            // 7. MUICIPALIDAD
                            if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -') {
                                auxArray[7] = objResult[i].getValue(columns[7]);
                            } else {
                                auxArray[7] = '0';
                            }

                        }
                        auxArray[6] = auxArrJson;
                    } else {
                        DbolStop = true;
                    }
                }
            }
            return auxArray;
        }

        function getMainInformation(id) {

            var intDMinReg = 0;
            var intDMaxReg = 1000;
            var DbolStop = false;
            var arrAuxiliar = new Array();
            var tasa, netAmount, retAmount, concepto;

            var transactionSearchObj = search.create({
                type: "transaction",
                filters: [
                    ["posting", "is", "T"],
                    "AND", ["voided", "is", "F"],
                    "AND", ["taxline", "is", "F"],
                    "AND", ["mainline", "is", "F"],
                    "AND", ["internalid", "anyof", id],
                ],
                columns: [
                    search.createColumn({
                        name: "internalid",
                        summary: "GROUP",
                        label: "0.Internal ID"
                    }),
                    search.createColumn({
                        name: "formulatext",
                        summary: "GROUP",
                        formula: columnaCodWht,
                        label: "1. Código WHT"
                    }),
                    search.createColumn({
                        name: "type",
                        summary: "GROUP",
                        label: "2. Tipo"
                    }),
                    search.createColumn({
                        name: "tranid",
                        summary: "GROUP",
                        label: "3. NO FACTURA"
                    }),
                    search.createColumn({
                        name: "formulacurrency",
                        summary: "SUM",
                        formula: "{amount}",
                        label: "4. NET AMOUNT"
                    }),
                    search.createColumn({
                        name: "formulacurrency",
                        summary: "GROUP",
                        formula: columnaWhtAmount,
                        label: "5. RET"
                    }),
                    search.createColumn({
                        name: "formulatext",
                        summary: "GROUP",
                        formula: columnaTasa,
                        label: "6. TASA"
                    })
                ],
                settings: [{
                    name: 'consolidationtype',
                    value: 'NONE'
                }]
            });

            if (param_Json.id_Multibook != '') {

                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [param_Json.id_multibook]
                });
                transactionSearchObj.filters.push(multibookFilter);

                var columna4 = search.createColumn({
                    name: "formulacurrency",
                    summary: "SUM",
                    formula: "{accountingtransaction.amount}",
                    label: "4. NET AMOUNT"
                });
                transactionSearchObj.columns.splice(4, 1, columna4);
            }

            if (param_Json.id_Tipo_de_retencion == 1) {
                //7.- Municipalidad
                var municipTransaction = search.createColumn({
                    name: "internalid",
                    join: "CUSTBODY_LMRY_MUNICIPALITY",
                    summary: "GROUP",
                    label: "7. Municipality"
                });
                transactionSearchObj.columns.push(municipTransaction);
            }

            var searchresult = transactionSearchObj.run();

            while (!DbolStop) {
                var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

                if (objResult != null) {
                    var intLength = objResult.length;
                    if (intLength != 1000) {
                        DbolStop = true;
                    }

                    for (var i = 0; i < intLength; i++) {
                        var columns = objResult[i].columns;
                        tasa = '', netAmount = '', retAmount = '', concepto = '';

                        arrAuxiliar = new Array();

                        //0. Internal ID
                        if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -') {
                            arrAuxiliar[0] = objResult[i].getValue(columns[0]);
                        } else {
                            arrAuxiliar[0] = '';
                        }

                        arrAuxiliar[1] = '';

                        // 1. Código WHT
                        if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -') {
                            concepto = objResult[i].getValue(columns[1]);
                        } else {
                            concepto = '';
                        }

                        // 2. Type
                        if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -') {
                            arrAuxiliar[2] = objResult[i].getValue(columns[2]);
                        } else {
                            arrAuxiliar[2] = '';
                        }

                        // 3. No Factura
                        if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -') {
                            arrAuxiliar[3] = objResult[i].getValue(columns[3]);
                        } else {
                            arrAuxiliar[3] = '';
                        }

                        arrAuxiliar[4] = '';
                        arrAuxiliar[5] = '';

                        // 4. NET AMOUNT
                        if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -') {
                            netAmount = String(Math.abs(objResult[i].getValue(columns[4])));
                        } else {
                            netAmount = '0.00';
                        }

                        // 5. RETENCION
                        if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -') {
                            if (objResult[i].getValue(columns[2]) == "VendCred") {
                                retAmount = String(Number(objResult[i].getValue(columns[5])) * (-1));
                            } else {
                                retAmount = objResult[i].getValue(columns[5]);
                            }
                        } else {
                            retAmount = '0.00';
                        }

                        // 6. TASA
                        if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -') {
                            tasa = objResult[i].getValue(columns[6]);
                        } else {
                            tasa = '';
                        }

                        // 6. JSON
                        arrAuxiliar[6] = [{
                            subtype: {
                                text: retType
                            },
                            lc_baseamount: netAmount,
                            lc_whtamount: retAmount,
                            whtrate: tasa,
                            description: concepto
                        }];

                        // 7. MUNICIPALIDAD
                        if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -') {
                            arrAuxiliar[7] = objResult[i].getValue(columns[7]);
                        } else {
                            arrAuxiliar[7] = '';
                        }
                    }
                } else {
                    DbolStop = true;
                }
            }

            return arrAuxiliar;

        }

        function getConcept(id, type_search, id_colum) {

            var customRecordJson = {};
            var concepto = '';

            var custom_Search = search.create({
                type: type_search,
                filters: [
                    ["isinactive", "is", "F"],
                    "AND", ["internalid", "anyof", id]
                ],
                columns: [
                    search.createColumn({ name: "internalid", label: "Internal ID" }),
                    search.createColumn({ name: id_colum, label: "Latam - Withholding Description" })
                ]
            });

            customRecordJson = GenerateJson.getCustomRecordJSON_v2(custom_Search);
            log.debug("customRecordJson", customRecordJson);

            concepto = customRecordJson[id]["Latam - Withholding Description"]

            return customRecordJson;

        }



        return {
            getInputData: getInputData,
            map: map,
            // reduce: reduce,
            summarize: summarize
        };

    });