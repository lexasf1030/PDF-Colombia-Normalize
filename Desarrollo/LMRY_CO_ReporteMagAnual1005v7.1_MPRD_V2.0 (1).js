/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_ReporteMagAnual1005v7.1_MPRD_V2.0.js     ||
||                                                              ||
||  Version Date           Author        Remarks                ||
||  2.0     Marzo 29 2019  LatamReady    Use Script 2.0         ||
 \= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */

/**
 *@NApiVersion 2.0
 *@NScriptType MapReduceScript
 *@NModuleScope Public
 */

define(["N/record", "N/runtime", "N/file", "N/email", "N/search", "N/encode", "N/currency", "N/format", "N/log", "N/config", "N/xml", "N/task", "./CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js", "/SuiteBundles/Bundle 37714/Latam_Library/LMRY_LibraryReport_LBRY_V2.js"], function(record, runtime, file, email, search, encode, currency, format, log, config, xml, task, libreria, libreriaGeneral) {

    var LMRY_script = "LMRY_CO_ReporteMagAnual1005v7.1_MPRD_V2.0.js";
    var objContext = runtime.getCurrentScript();

    var paramSubsidiaria = objContext.getParameter(
        "custscript_lmry_co_1005_anualv7_subsi"
    );
    var paramPeriodo = objContext.getParameter(
        "custscript_lmry_co_1005_anualv7_periodo"
    );
    var paramMultibook = objContext.getParameter(
        "custscript_lmry_co_1005_multi_anualv7"
    );
    var paramIdReport = objContext.getParameter(
        "custscript_lmry_co_1005_anualv7_idrepor"
    );
    var paramIdLog = objContext.getParameter(
        "custscript_lmry_co_1005_anualv7_idlog"
    );
    var paramConcepto = objContext.getParameter(
        "custscript_lmry_co_1005_anualv7_concept"
    );
    var paramCont = objContext.getParameter(
        "custscript_lmry_co_1005_anualv7_71_cont"
    );
    var paramIdFeatureByVersion = objContext.getParameter(
        "custscript_lmry_co_1005_anualv7_idfbv"
    );

    var isSubsidiariaFeature = runtime.isFeatureInEffect({
        feature: "SUBSIDIARIES",
    });

    var isMultibookFeature = runtime.isFeatureInEffect({
        feature: "MULTIBOOK",
    });

    var hasJobsFeature = runtime.isFeatureInEffect({
        feature: "JOBS",
    });

    var hasAdvancedJobsFeature = runtime.isFeatureInEffect({
        feature: "ADVANCEDJOBS",
    });

    var language = runtime
        .getCurrentScript()
        .getParameter("LANGUAGE")
        .substring(0, 2);

    function getInputData() {
        try {
            //11 - 1 - 2017 - 35 - 5 - 1 - 63808

            log.debug("parametros", paramSubsidiaria + " - " + paramMultibook + " - " + paramPeriodo + " - " + paramIdReport + " - " + paramIdFeatureByVersion + " - " + paramConcepto + " - " + paramIdLog);
            log.debug("hasJobsFeature -", hasJobsFeature);
            log.debug("hasAdvancedJobsFeature -", hasAdvancedJobsFeature);
            var TransaccArray1 = ObtenerVentasPorPagar1();
            var TaxResultJournalArray = getTaxResultJournals();
            var TransaccArray2 = ObtenerVentasPorPagar2();
            var TransacBillForeig = ObtenerRetencionesExtranjeras();

            log.debug("Num. Transacciones 1", TransaccArray1.length);
            log.debug("Num. TaxResultJournalArray", TaxResultJournalArray.length);
            log.debug("Num. Transacciones 2", TransaccArray2.length);
            log.debug("Num. Transacciones 3", TransacBillForeig.length);

            var transactionArray = TransaccArray1.concat(TaxResultJournalArray);
            var transactionArray = transactionArray.concat(TransaccArray2);
            transactionArray = transactionArray.concat(TransacBillForeig);

            if (transactionArray.length != 0) {
                return transactionArray;
            } else {
                NoData();
            }
        } catch (error) {
            log.debug("FIX ME", error);
            return [{
                isError: "T",
                error: error,
            }, ];
        }
    }

    function map(context) {
        try {
            var objResult = JSON.parse(context.value);
            //log.debug('objResult', objResult);
            if (objResult[objResult.length - 1] == "BILL EXTRANJERO") {
                var taxExtranjero = getTaxResultExtranjero(objResult);
                var accountDetailJson = getTransactionDetail(taxExtranjero);

            } else {
                var accountDetailJson = getTransactionDetail(objResult);
            }

            //log.debug('accountDetailJson', accountDetailJson);
            for (var key in accountDetailJson) {
                context.write({
                    key: key,
                    value: accountDetailJson[key],
                });
            }
        } catch (error) {
            log.debug("error map", error);
            log.debug("error objResult map", objResult);
            log.debug("error key", context.key);
            context.write({
                key: context.key,
                value: {
                    isError: "T",
                    error: error,
                },
            });
        }
    }

    function reduce(context) {}

    function summarize(context) {
        try {
            var i = 0;
            var json_final = {};
            var errores = [];

            context.output.iterator().each(function(key, value) {
                var objKey = key;
                var objResult = JSON.parse(value);

                if (objResult["isError"] == "T") {
                    errores.push(JSON.stringify(objResult["error"]));
                } else {
                    if (objResult[8] != 0 || objResult[9] != 0) {
                        if (json_final[objKey] != undefined) {
                            json_final[objKey][8] += objResult[8];
                            json_final[objKey][9] += objResult[9];
                        } else {
                            json_final[objKey] = objResult;
                        }
                        i++;
                    } else {
                        log.debug("Json_final", "no entra");
                    }
                }
                return true;
            });

            log.debug("Tamaño sumarize final", i);
            log.debug("ERRORES", errores.length);
            var json_finalArray = Object.keys(json_final);
            log.debug("Tamaño keys agrupado", json_finalArray.length);

            if (i != 0) {
                var numeroEnvio = obtenerNumeroEnvio();
                GenerarExcel(json_final, numeroEnvio);
                log.debug("Hace el ", "Excel");
                GenerarXml(json_final, numeroEnvio);
            } else {
                NoData();
            }
        } catch (error) {
            log.debug("error", error);
        }
    }

    function ObtenerVentasPorPagar1() {
        var savedSearch;

        savedSearch = search.load({
            id: "customsearch_lmry_co_form1005_v71_part_1",
        });

        if (paramPeriodo) {
            var periodStartDate = new Date(paramPeriodo, 0, 1);
            var periodEndDate = new Date(paramPeriodo, 11, 31);

            periodStartDate = format.format({
                value: periodStartDate,
                type: format.Type.DATE,
            });

            periodEndDate = format.format({
                value: periodEndDate,
                type: format.Type.DATE,
            });

            var fechInicioFilter = search.createFilter({
                name: "trandate",
                operator: search.Operator.ONORAFTER,
                values: [periodStartDate],
            });
            savedSearch.filters.push(fechInicioFilter);

            var fechFinFilter = search.createFilter({
                name: "trandate",
                operator: search.Operator.ONORBEFORE,
                values: [periodEndDate],
            });
            savedSearch.filters.push(fechFinFilter);
        }

        if (isSubsidiariaFeature) {
            var subsidiaryFilter = search.createFilter({
                name: "subsidiary",
                operator: search.Operator.IS,
                values: [paramSubsidiaria],
            });
            savedSearch.filters.push(subsidiaryFilter);
        }
        //"CASE WHEN CONCAT ({Type.id},'') = 'Check' THEN {mainname.id} ELSE NVL({vendor.internalid},{vendorline.internalid}) END"
        var vendorColumn = search.createColumn({
            name: "formulanumeric",
            summary: "GROUP",
            formula: "CASE WHEN CONCAT ({Type.id},'') = 'Check' THEN {mainname.id} ELSE NVL({vendor.internalid},{vendorline.internalid}) END",
        });
        savedSearch.columns.push(vendorColumn);


        "CASE WHEN CONCAT ({Type.id},'') = 'Journal' THEN {custbody_lmry_reference_entity.custentity_lmry_country.id} ELSE NVL({vendor.internalid},{vendorline.internalid}) END"

        //! F: Se encontro que los job (project) no se muestran en las lineas de detalle en las lineas de impuesto
        //* Se realizara una busqueda por el map

        if (hasJobsFeature && !hasAdvancedJobsFeature || hasJobsFeature && hasAdvancedJobsFeature) {
            log.debug('entra SI jobs', 'SI jobs');
            var customerColumn = search.createColumn({
                name: 'formulanumeric',
                formula: 'CASE WHEN NVL({job.internalid},-1) = -1 THEN {customer.internalid} ELSE {job.customer.id} end',
                summary: "GROUP"
            });
            savedSearch.columns.push(customerColumn);
        } else {
            log.debug('entra SIN jobs', 'NO jobs');
            var customerColumn = search.createColumn({
                name: "formulanumeric",
                formula: "{customer.internalid}",
                summary: "GROUP"
            });
            savedSearch.columns.push(customerColumn);
        }

        if (isMultibookFeature) {
            var multibookFilter = search.createFilter({
                name: "accountingbook",
                join: "accountingtransaction",
                operator: search.Operator.IS,
                values: [paramMultibook],
            });
            savedSearch.filters.push(multibookFilter);

            var formula1 =
                "CASE WHEN CONCAT ({Type.id},'') = 'CustCred' THEN 0 ELSE NVL({accountingtransaction.amount},0) END";

            var column1 = search.createColumn({
                name: "formulacurrency",
                summary: "SUM",
                formula: formula1,
            });
            savedSearch.columns.splice(1, 1, column1);

            var formula2 =
                "CASE WHEN CONCAT ({Type.id},'') = 'CustCred' THEN -NVL({accountingtransaction.amount},0) ELSE 0 END";

            var column2 = search.createColumn({
                name: "formulacurrency",
                summary: "SUM",
                formula: formula2,
            });
            savedSearch.columns.splice(2, 1, column2);

            var amountFilter = search.createFilter({
                name: 'formulanumeric',
                formula: "NVL({accountingtransaction.amount},0)",
                operator: search.Operator.NOTEQUALTO,
                values: 0
            });
            savedSearch.filters.push(amountFilter);

        } else {
            var amountFilter = search.createFilter({
                name: 'formulanumeric',
                formula: "NVL({amount},0)",
                operator: search.Operator.NOTEQUALTO,
                values: 0
            });
            savedSearch.filters.push(amountFilter);
        }

        var EmployeeColumn = search.createColumn({
            name: "formulanumeric",
            formula: "NVL({entity.id},{name.id})",
            summary: "GROUP",
        });
        savedSearch.columns.push(EmployeeColumn);

        var InternalId = search.createColumn({
            name: "formulanumeric",
            formula: "{internalid}",
            summary: "GROUP",
        });
        savedSearch.columns.push(InternalId);

        var pagedData = savedSearch.runPaged({
            pageSize: 1000,
        });

        var page,
            transactionsArray = [];
        var cont0 = 0;
        var cont1 = 0;

        pagedData.pageRanges.forEach(function(pageRange) {
            page = pagedData.fetch({
                index: pageRange.index,
            });

            page.data.forEach(function(result) {
                //cont1++;
                //if (Number(result.getValue(result.columns[1])) != 0 || Number(result.getValue(result.columns[2])) != 0) {
                var rowArray = [];
                // 0. TIPO
                if (result.getValue(result.columns[0]) != "- None -" && result.getValue(result.columns[0]) != "" && result.getValue(result.columns[0]) != null) {
                    rowArray[0] = result.getValue(result.columns[0]);
                } else {
                    rowArray[0] = "";
                }

                // 1. IMPUESTO GENERADO
                if (result.getValue(result.columns[1]) != "- None -" && result.getValue(result.columns[1]) != "" && result.getValue(result.columns[1]) != null) {
                    rowArray[1] = Math.abs(Number(result.getValue(result.columns[1])));
                } else {
                    rowArray[1] = 0;
                }

                // 2. IVA RECUPERADO EN DEVOLUCIONES EN COMPRAS ANULADAS, RESCINDIDAS O RESUELTAS
                if (result.getValue(result.columns[2]) != "- None -" && result.getValue(result.columns[2]) != "" && result.getValue(result.columns[2]) != null) {
                    rowArray[2] = Math.abs(Number(result.getValue(result.columns[2])));
                } else {
                    rowArray[2] = 0;
                }

                // 3. VENDOR
                if (result.getValue(result.columns[3]) != "- None -" && result.getValue(result.columns[3]) != "" && result.getValue(result.columns[3]) != null) {
                    rowArray[3] = result.getValue(result.columns[3]);
                } else {
                    rowArray[3] = "";
                }

                // 4. CUSTOMER
                if (result.getValue(result.columns[4]) != "- None -" && result.getValue(result.columns[4]) != "" && result.getValue(result.columns[4]) != null) {
                    rowArray[4] = result.getValue(result.columns[4]);
                } else {
                    rowArray[4] = "";
                }

                // 5. ENTITY
                if (result.getValue(result.columns[5]) != "- None -" && result.getValue(result.columns[5]) != "" && result.getValue(result.columns[5]) != null) {
                    rowArray[5] = result.getValue(result.columns[5]);
                } else {
                    rowArray[5] = "";
                }

                // 5. INTERNAL ID
                if (result.getValue(result.columns[6]) != "- None -" && result.getValue(result.columns[6]) != "" && result.getValue(result.columns[6]) != null) {
                    rowArray[6] = result.getValue(result.columns[6]);
                } else {
                    rowArray[6] = "";
                }

                transactionsArray.push(rowArray);
                // } else {
                //     cont0++;
                // }
            });
        });
        log.debug("num de cuentas con monto", cont1);
        log.debug("num de cuentas con monto 0", cont0);
        return transactionsArray;
    }

    function getTaxResultJournals() {
        var savedSearch;

        var savedSearch = search.create({
            type: "journalentry",
            filters: [
                ["posting", "is", "T"],
                "AND", ["voided", "is", "F"],
                "AND", ["memorized", "is", "F"],
                "AND", ["type", "anyof", "Journal"],
                "AND", ["formulatext: CASE WHEN {lineuniquekey} = {custrecord_lmry_br_transaction.custrecord_lmry_lineuniquekey} THEN 1 ELSE 0 END", "is", "1"],
                "AND", ["formulatext: CASE WHEN NVL({custrecord_lmry_br_transaction.custrecord_lmry_tax_type.id},0) = 4 THEN 1 ELSE 0 END", "is", "1"]
            ],
            columns: [
                search.createColumn({
                    name: "type",
                    summary: "GROUP",
                    label: "Type"
                }),
                search.createColumn({
                    name: "formulacurrency",
                    summary: "SUM",
                    formula: "{custrecord_lmry_br_transaction.custrecord_lmry_amount_local_currency}",
                    label: "Formula (Currency)"
                }),
                search.createColumn({
                    name: "formulacurrency",
                    summary: "SUM",
                    formula: "0",
                    label: "Formula (Currency)"
                })
            ]
        });

        if (paramPeriodo) {
            var periodStartDate = new Date(paramPeriodo, 0, 1);
            var periodEndDate = new Date(paramPeriodo, 11, 31);

            periodStartDate = format.format({
                value: periodStartDate,
                type: format.Type.DATE,
            });

            periodEndDate = format.format({
                value: periodEndDate,
                type: format.Type.DATE,
            });

            var fechInicioFilter = search.createFilter({
                name: "trandate",
                operator: search.Operator.ONORAFTER,
                values: [periodStartDate],
            });
            savedSearch.filters.push(fechInicioFilter);

            var fechFinFilter = search.createFilter({
                name: "trandate",
                operator: search.Operator.ONORBEFORE,
                values: [periodEndDate],
            });
            savedSearch.filters.push(fechFinFilter);
        }

        if (isSubsidiariaFeature) {
            var subsidiaryFilter = search.createFilter({
                name: "subsidiary",
                operator: search.Operator.IS,
                values: [paramSubsidiaria],
            });
            savedSearch.filters.push(subsidiaryFilter);
        }
        //"CASE WHEN CONCAT ({Type.id},'') = 'Check' THEN {mainname.id} ELSE NVL({vendor.internalid},{vendorline.internalid}) END"
        var vendorColumn = search.createColumn({
            name: "formulanumeric",
            summary: "GROUP",
            formula: "CASE WHEN CONCAT ({Type.id},'') = 'Check' THEN {mainname.id} ELSE NVL({vendor.internalid},{vendorline.internalid}) END",
        });
        savedSearch.columns.push(vendorColumn);

        if (hasJobsFeature && !hasAdvancedJobsFeature || hasJobsFeature && hasAdvancedJobsFeature) {
            log.debug('entra SI jobs', 'SI jobs');
            var customerColumn = search.createColumn({
                name: 'formulanumeric',
                formula: 'CASE WHEN NVL({job.internalid},-1) = -1 THEN {customer.internalid} ELSE {job.customer.id} end',
                summary: "GROUP"
            });
            savedSearch.columns.push(customerColumn);
        } else {
            log.debug('entra SIN jobs', 'NO jobs');
            var customerColumn = search.createColumn({
                name: "formulanumeric",
                formula: "{customer.internalid}",
                summary: "GROUP"
            });
            savedSearch.columns.push(customerColumn);
        }

        if (isMultibookFeature) {
            var multibookFilter = search.createFilter({
                name: "accountingbook",
                join: "accountingtransaction",
                operator: search.Operator.IS,
                values: [paramMultibook],
            });
            savedSearch.filters.push(multibookFilter);

            var formula1 =
                "CASE WHEN CONCAT ({Type.id},'') = 'CustCred' THEN 0 ELSE CASE WHEN CONCAT ({Type.id},'') = 'Journal' THEN {custrecord_lmry_br_transaction.custrecord_lmry_amount_local_currency} ELSE NVL({accountingtransaction.amount},0) END END";

            var column1 = search.createColumn({
                name: "formulacurrency",
                summary: "SUM",
                formula: formula1,
            });
            //savedSearch.columns.splice(1, 1, column1);

            var formula2 =
                "CASE WHEN CONCAT ({Type.id},'') = 'CustCred' THEN -NVL({accountingtransaction.amount},0) ELSE 0 END";

            var column2 = search.createColumn({
                name: "formulacurrency",
                summary: "SUM",
                formula: formula2,
            });
            //savedSearch.columns.splice(2, 1, column2);
        }

        var EmployeeColumn = search.createColumn({
            name: "formulanumeric",
            formula: "NVL({entity.id},{name.id})",
            summary: "GROUP",
        });
        savedSearch.columns.push(EmployeeColumn);

        var InternalId = search.createColumn({
            name: "formulanumeric",
            formula: "{internalid}",
            summary: "GROUP",
        });
        savedSearch.columns.push(InternalId);

        var pagedData = savedSearch.runPaged({
            pageSize: 1000,
        });

        var page,
            transactionsArray = [];
        var cont0 = 0;
        var cont1 = 0;

        pagedData.pageRanges.forEach(function(pageRange) {
            page = pagedData.fetch({
                index: pageRange.index,
            });

            page.data.forEach(function(result) {
                cont1++;
                if (
                    Number(result.getValue(result.columns[1])) != 0 ||
                    Number(result.getValue(result.columns[2])) != 0
                ) {
                    var rowArray = [];
                    // 0. TIPO
                    if (
                        result.getValue(result.columns[0]) != "- None -" &&
                        result.getValue(result.columns[0]) != "" &&
                        result.getValue(result.columns[0]) != null
                    ) {
                        rowArray[0] = result.getValue(result.columns[0]);
                    } else {
                        rowArray[0] = "";
                    }

                    // 1. IMPUESTO GENERADO
                    if (
                        result.getValue(result.columns[1]) != "- None -" &&
                        result.getValue(result.columns[1]) != "" &&
                        result.getValue(result.columns[1]) != null
                    ) {
                        rowArray[1] = Math.abs(Number(result.getValue(result.columns[1])));
                    } else {
                        rowArray[1] = 0;
                    }

                    // 2. IVA RECUPERADO EN DEVOLUCIONES EN COMPRAS ANULADAS, RESCINDIDAS O RESUELTAS
                    if (
                        result.getValue(result.columns[2]) != "- None -" &&
                        result.getValue(result.columns[2]) != "" &&
                        result.getValue(result.columns[2]) != null
                    ) {
                        rowArray[2] = Math.abs(Number(result.getValue(result.columns[2])));
                    } else {
                        rowArray[2] = 0;
                    }

                    // 3. VENDOR
                    if (
                        result.getValue(result.columns[3]) != "- None -" &&
                        result.getValue(result.columns[3]) != "" &&
                        result.getValue(result.columns[3]) != null
                    ) {
                        rowArray[3] = result.getValue(result.columns[3]);
                    } else {
                        rowArray[3] = "";
                    }

                    // 4. CUSTOMER
                    if (
                        result.getValue(result.columns[4]) != "- None -" &&
                        result.getValue(result.columns[4]) != "" &&
                        result.getValue(result.columns[4]) != null
                    ) {
                        rowArray[4] = result.getValue(result.columns[4]);
                    } else {
                        rowArray[4] = "";
                    }

                    // 5. ENTITY
                    if (
                        result.getValue(result.columns[5]) != "- None -" &&
                        result.getValue(result.columns[5]) != "" &&
                        result.getValue(result.columns[5]) != null
                    ) {
                        rowArray[5] = result.getValue(result.columns[5]);
                    } else {
                        rowArray[5] = "";
                    }

                    // 5. INTERNAL ID
                    if (
                        result.getValue(result.columns[6]) != "- None -" &&
                        result.getValue(result.columns[6]) != "" &&
                        result.getValue(result.columns[6]) != null
                    ) {
                        rowArray[6] = result.getValue(result.columns[6]);
                    } else {
                        rowArray[6] = "";
                    }

                    transactionsArray.push(rowArray);
                } else {
                    cont0++;
                }
            });
        });
        log.debug("num de cuentas con monto", cont1);
        log.debug("num de cuentas con monto 0", cont0);
        return transactionsArray;
    }

    function ObtenerVentasPorPagar2() {
        var savedSearch;

        //! Nombre anterior: LatamReady - CO Form 1005 Impuesto a las ventas por pagar (Descontable) 7.1 P2
        //* Nombre actual: LatamReady - CO Form 1005 taxes P2

        savedSearch = search.load({
            id: "customsearch_lmry_co_form1005_v71_part_2",
        });

        //var accountsIdArray = ObtenerCuentas();
        var cuentasFormtMM = ObtenerCuentasFormatoMM();

        log.debug("cuentasFormtMM", cuentasFormtMM);
        //log.debug("cuentasFormtMM", cuentasFormtMM);

        if (paramPeriodo) {
            var periodStartDate = new Date(paramPeriodo, 0, 1);
            var periodEndDate = new Date(paramPeriodo, 11, 31);

            periodStartDate = format.format({
                value: periodStartDate,
                type: format.Type.DATE,
            });

            periodEndDate = format.format({
                value: periodEndDate,
                type: format.Type.DATE,
            });

            var fechInicioFilter = search.createFilter({
                name: "trandate",
                operator: search.Operator.ONORAFTER,
                values: [periodStartDate],
            });
            savedSearch.filters.push(fechInicioFilter);

            var fechFinFilter = search.createFilter({
                name: "trandate",
                operator: search.Operator.ONORBEFORE,
                values: [periodEndDate],
            });
            savedSearch.filters.push(fechFinFilter);
        }

        if (isSubsidiariaFeature) {
            var subsidiaryFilter = search.createFilter({
                name: "subsidiary",
                operator: search.Operator.IS,
                values: [paramSubsidiaria],
            });
            savedSearch.filters.push(subsidiaryFilter);
        }

        var vendorColumn = search.createColumn({
            name: "formulanumeric",
            summary: "GROUP",
            formula: "CASE WHEN CONCAT ({Type.id},'') = 'Check' THEN {mainname.id} ELSE NVL({vendor.internalid},{vendorline.internalid}) END",
        });
        savedSearch.columns.push(vendorColumn);

        if (hasJobsFeature && !hasAdvancedJobsFeature || hasJobsFeature && hasAdvancedJobsFeature) {
            log.debug('entra SI jobs', 'SI jobs');
            var customerColumn = search.createColumn({
                name: 'formulanumeric',
                formula: 'CASE WHEN NVL({job.internalid},-1) = -1 THEN {customer.internalid} ELSE {job.customer.id} end',
                summary: "GROUP"
            });
            savedSearch.columns.push(customerColumn);
        } else {
            log.debug('entra SIN jobs', 'NO jobs');
            var customerColumn = search.createColumn({
                name: "formulanumeric",
                formula: "{customer.internalid}",
                summary: "GROUP"
            });
            savedSearch.columns.push(customerColumn);
        }

        if (isMultibookFeature) {
            // if (accountsIdArray.length != 0) {
            //     var accountFilter = search.createFilter({
            //         name: "account",
            //         join: "accountingtransaction",
            //         operator: search.Operator.ANYOF,
            //         values: accountsIdArray,
            //     });
            //     savedSearch.filters.splice(1, 0, accountFilter);
            //     savedSearch.filters.splice(9, 0, accountFilter);
            //     savedSearch.filters.splice(18, 0, accountFilter);
            //     savedSearch.filters.splice(24, 0, accountFilter);
            // } else {
            //     log.debug("accountsIdArray es 0", "accountsIdArray es 0");
            //     var accountFilter0 = search.createFilter({
            //         name: "account",
            //         join: "accountingtransaction",
            //         operator: search.Operator.ISEMPTY,
            //     });
            //     savedSearch.filters.splice(1, 0, accountFilter0);
            //     savedSearch.filters.splice(9, 0, accountFilter0);
            //     savedSearch.filters.splice(18, 0, accountFilter0);
            //     savedSearch.filters.splice(24, 0, accountFilter0);
            // }

            var amountFilter = search.createFilter({
                name: "formulatext",
                operator: search.Operator.IS,
                formula: "CASE WHEN NVL({accountingtransaction.debitamount},0)-NVL({accountingtransaction.creditamount},0) = 0 THEN 0 ELSE 1 END",
                values: "1",
            });
            savedSearch.filters.splice(8, 1, amountFilter);

            var multibookFilter = search.createFilter({
                name: "accountingbook",
                join: "accountingtransaction",
                operator: search.Operator.IS,
                values: [paramMultibook],
            });
            savedSearch.filters.push(multibookFilter);

            if (cuentasFormtMM.length != 0) {
                var accountFilterMM = search.createFilter({
                    name: "account",
                    join: "accountingtransaction",
                    operator: search.Operator.ANYOF,
                    values: cuentasFormtMM,
                });
                savedSearch.filters.push(accountFilterMM);
            } else {
                log.debug("cuentasFormtMM es 0", "cuentasFormtMM es 0");
                var accountFilterMM0 = search.createFilter({
                    name: "account",
                    join: "accountingtransaction",
                    operator: search.Operator.ISEMPTY,
                });
                savedSearch.filters.push(accountFilterMM0);
            }

            var part1 =
                "CASE WHEN {abbrev}='GENJRNL' AND {custbody_lmry_apply_wht_code} = 'T' THEN 0 WHEN {accountingtransaction.account.id} in (";
            var part2 = cuentasFormtMM.join() || "-1";
            var part3 =
                ") THEN NVL({accountingtransaction.debitamount},0) - NVL({accountingtransaction.creditamount},0) ELSE 0 END";

            var formula = part1 + part2 + part3;

            log.debug("formula", formula);

            var column1 = search.createColumn({
                name: "formulacurrency",
                summary: "SUM",
                formula: formula,
            });
            savedSearch.columns.splice(1, 1, column1);

            var formula2 =
                "CASE WHEN {abbrev}='GENJRNL' AND {custbody_lmry_apply_wht_code} = 'T'  THEN NVL({accountingtransaction.debitamount},0) - NVL({accountingtransaction.creditamount},0) ELSE 0 END";

            log.debug("formula2", formula2);
            var column2 = search.createColumn({
                name: "formulacurrency",
                summary: "SUM",
                formula: formula2,
            });
            savedSearch.columns.splice(2, 1, column2);
        } else {
            // if (accountsIdArray.length != 0) {
            //     var accountFilter = search.createFilter({
            //         name: "account",
            //         operator: search.Operator.ANYOF,
            //         values: accountsIdArray,
            //     });
            //     savedSearch.filters.splice(1, 0, accountFilter);
            //     savedSearch.filters.splice(9, 0, accountFilter);
            //     savedSearch.filters.splice(18, 0, accountFilter);
            //     savedSearch.filters.splice(24, 0, accountFilter);
            // } else {
            //     log.debug("accountsIdArray es 0", "accountsIdArray es 0");
            //     var accountFilter0 = search.createFilter({
            //         name: "account",
            //         operator: search.Operator.ISEMPTY,
            //     });
            //     savedSearch.filters.splice(1, 0, accountFilter0);
            //     savedSearch.filters.splice(9, 0, accountFilter0);
            //     savedSearch.filters.splice(18, 0, accountFilter0);
            //     savedSearch.filters.splice(24, 0, accountFilter0);
            // }

            if (cuentasFormtMM.length != 0) {
                log.debug("cuentasFormtMM es 0", "cuentasFormtMM es 0");
                var accountFilterMM = search.createFilter({
                    name: "account",
                    operator: search.Operator.ANYOF,
                    values: cuentasFormtMM,
                });
                savedSearch.filters.push(accountFilterMM);
            } else {
                var accountFilterMM0 = search.createFilter({
                    name: "account",
                    join: "accountingtransaction",
                    operator: search.Operator.ISEMPTY,
                });
                savedSearch.filters.push(accountFilterMM0);
            }

            var part1 =
                "CASE WHEN {abbrev}='GENJRNL' AND {custbody_lmry_apply_wht_code} = 'T' THEN 0 WHEN {account.id} in (";
            var part2 = cuentasFormtMM.join() || "-1";
            var part3 =
                ") THEN NVL({debitamount},0) - NVL({creditamount},0) ELSE 0 END";

            var formula = part1 + part2 + part3;

            log.debug("formula", formula);

            var column1 = search.createColumn({
                name: "formulacurrency",
                summary: "SUM",
                formula: formula,
            });
            savedSearch.columns.splice(1, 1, column1);
        }

        var EmployeeColumn = search.createColumn({
            name: "formulanumeric",
            formula: "{entity.id}",
            summary: "GROUP",
        });
        savedSearch.columns.push(EmployeeColumn);

        var pagedData = savedSearch.runPaged({
            pageSize: 1000,
        });

        var page,
            transactionsArray = [];
        var cont0 = 0;

        pagedData.pageRanges.forEach(function(pageRange) {
            page = pagedData.fetch({
                index: pageRange.index,
            });

            page.data.forEach(function(result) {
                if (
                    Number(result.getValue(result.columns[1])) != 0 ||
                    Number(result.getValue(result.columns[2])) != 0
                ) {
                    var rowArray = [];
                    // 0. TIPO
                    if (
                        result.getValue(result.columns[0]) != "- None -" &&
                        result.getValue(result.columns[0]) != "" &&
                        result.getValue(result.columns[0]) != null
                    ) {
                        rowArray[0] = result.getValue(result.columns[0]);
                    } else {
                        rowArray[0] = "";
                    }

                    // 1. IMPUESTO GENERADO
                    if (
                        result.getValue(result.columns[1]) != "- None -" &&
                        result.getValue(result.columns[1]) != "" &&
                        result.getValue(result.columns[1]) != null
                    ) {
                        rowArray[1] = Math.abs(Number(result.getValue(result.columns[1])));
                    } else {
                        rowArray[1] = 0;
                    }

                    // 2. IVA RECUPERADO EN DEVOLUCIONES EN COMPRAS ANULADAS, RESCINDIDAS O RESUELTAS
                    if (
                        result.getValue(result.columns[2]) != "- None -" &&
                        result.getValue(result.columns[2]) != "" &&
                        result.getValue(result.columns[2]) != null
                    ) {
                        rowArray[2] = Math.abs(Number(result.getValue(result.columns[2])));
                    } else {
                        rowArray[2] = 0;
                    }

                    // 3. VENDOR
                    if (
                        result.getValue(result.columns[3]) != "- None -" &&
                        result.getValue(result.columns[3]) != "" &&
                        result.getValue(result.columns[3]) != null
                    ) {
                        rowArray[3] = result.getValue(result.columns[3]);
                    } else {
                        rowArray[3] = "";
                    }

                    // 4. CUSTOMER
                    if (
                        result.getValue(result.columns[4]) != "- None -" &&
                        result.getValue(result.columns[4]) != "" &&
                        result.getValue(result.columns[4]) != null
                    ) {
                        rowArray[4] = result.getValue(result.columns[4]);
                    } else {
                        rowArray[4] = "";
                    }

                    // 5. ENTITY
                    if (
                        result.getValue(result.columns[5]) != "- None -" &&
                        result.getValue(result.columns[5]) != "" &&
                        result.getValue(result.columns[5]) != null
                    ) {
                        rowArray[5] = result.getValue(result.columns[5]);
                    } else {
                        rowArray[5] = "";
                    }

                    transactionsArray.push(rowArray);
                } else {
                    cont0++;
                }
            });
        });
        log.debug("num de cuentas con monto 0", cont0);
        return transactionsArray;
    }

    function ObtenerBillsExtranjeros() {
        var savedSearch;

        savedSearch = search.load({
            id: "customsearch_lmry_co_form1005_bill_f",
        });

        if (paramPeriodo) {
            var periodStartDate = new Date(paramPeriodo, 0, 1);
            var periodEndDate = new Date(paramPeriodo, 11, 31);

            periodStartDate = format.format({
                value: periodStartDate,
                type: format.Type.DATE,
            });

            periodEndDate = format.format({
                value: periodEndDate,
                type: format.Type.DATE,
            });

            var fechInicioFilter = search.createFilter({
                name: "trandate",
                operator: search.Operator.ONORAFTER,
                values: [periodStartDate],
            });
            savedSearch.filters.push(fechInicioFilter);

            var fechFinFilter = search.createFilter({
                name: "trandate",
                operator: search.Operator.ONORBEFORE,
                values: [periodEndDate],
            });
            savedSearch.filters.push(fechFinFilter);
        }

        if (isSubsidiariaFeature) {
            var subsidiaryFilter = search.createFilter({
                name: "subsidiary",
                operator: search.Operator.IS,
                values: [paramSubsidiaria],
            });
            savedSearch.filters.push(subsidiaryFilter);
        }

        var vendorColumn = search.createColumn({
            name: "formulanumeric",
            summary: "GROUP",
            formula: "NVL({vendor.internalid},{vendorline.internalid})",
        });
        savedSearch.columns.push(vendorColumn);

        if (isMultibookFeature) {
            var multibookFilter = search.createFilter({
                name: "accountingbook",
                join: "accountingtransaction",
                operator: search.Operator.IS,
                values: [paramMultibook],
            });
            savedSearch.filters.push(multibookFilter);
        }

        var pagedData = savedSearch.runPaged({
            pageSize: 1000,
        });

        var page,
            transactionsArray = [];
        var cont0 = 0;
        var rate = 0;

        pagedData.pageRanges.forEach(function(pageRange) {
            page = pagedData.fetch({
                index: pageRange.index,
            });

            page.data.forEach(function(result) {
                if (Number(result.getValue(result.columns[2])) != 0) {
                    var rowArray = [];
                    // 0. TIPO
                    if (
                        result.getValue(result.columns[0]) != "- None -" &&
                        result.getValue(result.columns[0]) != "" &&
                        result.getValue(result.columns[0]) != null
                    ) {
                        rowArray[0] = result.getValue(result.columns[0]);
                    } else {
                        rowArray[0] = "";
                    }

                    //RATE
                    if (
                        result.getValue(result.columns[1]) != "- None -" &&
                        result.getValue(result.columns[1]) != "" &&
                        result.getValue(result.columns[1]) != null
                    ) {
                        rate = result.getValue(result.columns[1]);
                    } else {
                        rate = "";
                    }

                    // 1. IMPUESTO DESCONTABLE
                    if (
                        result.getValue(result.columns[2]) != "- None -" &&
                        result.getValue(result.columns[2]) != "" &&
                        result.getValue(result.columns[2]) != null
                    ) {
                        rowArray[1] = Math.abs(Number(result.getValue(result.columns[2])));
                    } else {
                        rowArray[1] = 0;
                    }

                    // 2. IVA X DEVOLUCION
                    rowArray[2] = 0;

                    // 3. VENDOR
                    if (
                        result.getValue(result.columns[3]) != "- None -" &&
                        result.getValue(result.columns[3]) != "" &&
                        result.getValue(result.columns[3]) != null
                    ) {
                        rowArray[3] = result.getValue(result.columns[3]);
                    } else {
                        rowArray[3] = "";
                    }

                    // 4. CUSTOMER
                    rowArray[4] = "";

                    // 5. ENTITY
                    rowArray[5] = "";
                    log.debug('rowArray', rowArray);
                    transactionsArray.push(rowArray);
                } else {
                    cont0++;
                }
            });
        });
        log.debug("bill extranjero con monto 0", transactionsArray);
        return transactionsArray;
    }

    function ObtenerRetencionesExtranjeras() {
        var savedSearch;

        savedSearch = search.load({
            id: "customsearch_lmry_co_retention_ext",
        });

        if (paramPeriodo) {
            var periodStartDate = new Date(paramPeriodo, 0, 1);
            var periodEndDate = new Date(paramPeriodo, 11, 31);

            periodStartDate = format.format({
                value: periodStartDate,
                type: format.Type.DATE,
            });

            periodEndDate = format.format({
                value: periodEndDate,
                type: format.Type.DATE,
            });

            var fechInicioFilter = search.createFilter({
                name: "trandate",
                operator: search.Operator.ONORAFTER,
                values: [periodStartDate],
            });
            savedSearch.filters.push(fechInicioFilter);

            var fechFinFilter = search.createFilter({
                name: "trandate",
                operator: search.Operator.ONORBEFORE,
                values: [periodEndDate],
            });
            savedSearch.filters.push(fechFinFilter);
        }

        if (isSubsidiariaFeature) {
            var subsidiaryFilter = search.createFilter({
                name: "subsidiary",
                operator: search.Operator.IS,
                values: [paramSubsidiaria],
            });
            savedSearch.filters.push(subsidiaryFilter);
        }

        if (isMultibookFeature) {
            var multibookFilter = search.createFilter({
                name: "accountingbook",
                join: "accountingtransaction",
                operator: search.Operator.IS,
                values: [paramMultibook],
            });
            savedSearch.filters.push(multibookFilter);
        }

        var pagedData = savedSearch.runPaged({
            pageSize: 1000,
        });

        var page,
            transactionsArray = [];

        pagedData.pageRanges.forEach(function(pageRange) {
            page = pagedData.fetch({
                index: pageRange.index,
            });

            page.data.forEach(function(result) {

                var rowArray = [];
                // 0. TIPO
                if (result.getValue(result.columns[0]) != "- None -" && result.getValue(result.columns[0]) != "" && result.getValue(result.columns[0]) != null) {
                    rowArray[0] = result.getValue(result.columns[0]);
                } else {
                    rowArray[0] = "";
                }

                //1 . ID TRANSACCION ORIGEN
                if (result.getValue(result.columns[1]) != "- None -" && result.getValue(result.columns[1]) != "" && result.getValue(result.columns[1]) != null) {
                    rowArray[1] = result.getValue(result.columns[1]);
                } else {
                    rowArray[1] = "";
                }

                // 2. ID PERIODO TRANSACCIONE ORIGEN 
                if (result.getValue(result.columns[2]) != "- None -" && result.getValue(result.columns[2]) != "" && result.getValue(result.columns[2]) != null) {
                    rowArray[2] = getYearFromPeriod(result.getValue(result.columns[2]));
                } else {
                    rowArray[2] = "";
                }

                // 3. VENDOR
                if (result.getValue(result.columns[3]) != "- None -" && result.getValue(result.columns[3]) != "" && result.getValue(result.columns[3]) != null) {
                    rowArray[3] = result.getValue(result.columns[3]);
                } else {
                    rowArray[3] = "";
                }

                // 4. Identificador
                rowArray[4] = "BILL EXTRANJERO";


                log.debug('rowArray', rowArray);
                transactionsArray.push(rowArray);

            });
        });
        log.debug("retenciones extranjeras", transactionsArray);
        return transactionsArray;
    }

    function getTaxResultExtranjero(objResult) {
        //var objResult = ['VendBill', '5425493', 2016, '7155', 'BILL EXTRANJERO']
        var idOrigen = objResult[1];

        //TODO: OBTENER TAX RECLASIFICADO
        var tax_result_reclasf = ObtenerTaxReclasificado(idOrigen)
        log.debug('tax_result_reclasf', tax_result_reclasf);

        //TODO: OBTENER EL TAX RESULT RETEIVA DE LA TRANSACCION ORIGEN y ACTUALIZA LOS PERIODOS con tax_result_reclasf
        var tax_result_actualizado = ObtenerDatosTaxResult(objResult, tax_result_reclasf);
        log.debug('tax_result_actualizado', tax_result_actualizado);

        return tax_result_actualizado;

    }


    function ObtenerDatosTaxResult(objResult, json_reclasf) {

        var id_transatcion = objResult[1];
        var periodo_original = objResult[2];

        var customrecord_lmry_br_transactionSearchObj = search.create({
            type: "customrecord_lmry_br_transaction",
            filters: [
                ["formulatext: CASE WHEN NVL({custrecord_lmry_br_type},'') = 'ReteIVA' OR NVL({custrecord_lmry_br_type},'') = 'Auto ReteIVA' THEN 1 ELSE 0 END", "is", "1"],
                "AND", ["custrecord_lmry_br_transaction.mainline", "is", "T"],
                "AND", ["custrecord_lmry_br_transaction.internalid", "anyof", id_transatcion]
            ],
            columns: [
                search.createColumn({
                    name: "formulanumeric",
                    formula: "{internalid}",
                    label: "Formula (Numeric)"
                }),
                search.createColumn({
                    name: "formulanumeric",
                    formula: "NVL({custrecord_lmry_amount_local_currency},0)",
                    label: "Formula (Numeric)"
                })
            ]
        });

        var pagedData = customrecord_lmry_br_transactionSearchObj.runPaged({
            pageSize: 1000,
        });

        var page,
            transactionsArray = [],
            sumaAmount = 0;

        pagedData.pageRanges.forEach(function(pageRange) {
            page = pagedData.fetch({
                index: pageRange.index,
            });

            page.data.forEach(function(result) {

                var rowArray = [];
                // 0. ID tax
                if (result.getValue(result.columns[0]) != "- None -" && result.getValue(result.columns[0]) != "" && result.getValue(result.columns[0]) != null) {
                    rowArray[0] = result.getValue(result.columns[0]);
                } else {
                    rowArray[0] = "";
                }

                //1 . ret amount
                if (result.getValue(result.columns[1]) != "- None -" && result.getValue(result.columns[1]) != "" && result.getValue(result.columns[1]) != null) {
                    rowArray[1] = result.getValue(result.columns[1]);
                } else {
                    rowArray[1] = "";
                }

                if (json_reclasf[rowArray[0]] != undefined) {
                    var periodoActual = json_reclasf[rowArray[0]];
                } else {
                    var periodoActual = periodo_original;
                }

                if (periodoActual == paramPeriodo) {
                    sumaAmount += Number(rowArray[1]);
                    //transactionsArray.push(rowArray);
                }

            });
        });

        var taxResulAgrupado = [];
        // 0. TIPO
        taxResulAgrupado[0] = objResult[0];
        // 1. IMPUESTO DESCONTABLE
        taxResulAgrupado[1] = sumaAmount;
        // 2. IVA X DEVOLUCION
        taxResulAgrupado[2] = 0;
        // 3. VENDOR
        taxResulAgrupado[3] = objResult[3];
        // 4. CUSTOMER
        taxResulAgrupado[4] = '';
        // 5. ENTITY
        taxResulAgrupado[5] = '';

        return taxResulAgrupado;
    }


    function ObtenerTaxReclasificado(idOrigen) {

        var customrecord_lmry_co_wht_reclasification_search = search.create({
            type: "customrecord_lmry_co_wht_reclasification",
            filters: [
                ["custrecord_co_reclasification_data", "contains", idOrigen],
                "AND", ["custrecord_co_reclasification_status", "is", "Complete"],
                "AND", ["custrecord_co_reclasification_return", "isnot", "{}"]
            ],
            columns: [
                search.createColumn({ name: "custrecord_co_reclasification_return", label: "Latam - Data Return" }),
                search.createColumn({
                    name: "internalid",
                    join: "CUSTRECORD_CO_RECLASIFICATION_PERIOD",
                    label: "Internal ID"
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

                var jsonTaxRe_aux = {};
                var periodYear = '';
                // 0. DATA
                if (result.getValue(result.columns[0]) != "- None -" && result.getValue(result.columns[0]) != "" && result.getValue(result.columns[0]) != null) {
                    jsonTaxRe_aux = JSON.parse(result.getValue(result.columns[0]));
                } else {
                    jsonTaxRe_aux = {};
                }

                //1 . ID TRANSACCION ORIGEN
                if (result.getValue(result.columns[1]) != "- None -" && result.getValue(result.columns[1]) != "" && result.getValue(result.columns[1]) != null) {
                    periodYear = getYearFromPeriod(result.getValue(result.columns[1]));
                } else {
                    periodYear = '';
                }

                jsonTaxRe_aux['year'] = periodYear;

                transactionsArray.push(jsonTaxRe_aux);

            });
        });

        var json_final = {};

        for (var i = 0; i < transactionsArray.length; i++) {
            var aux_tax = transactionsArray[i][idOrigen];
            for (var j = 0; j < aux_tax.length; j++) {
                //arr_final.push[aux_tax[j]['taxResult'],transactionsArray[i]['year']]
                json_final[aux_tax[j]['taxResult'] + ''] = transactionsArray[i]['year'];
            }
        }

        return json_final;
    }

    function getYearFromPeriod(idPeriod) {
        var fechaIni = search.lookupFields({
            type: search.Type.ACCOUNTING_PERIOD,
            id: idPeriod,
            columns: ['startdate']
        }).startdate;

        var year = format.parse({
            value: fechaIni,
            type: format.Type.DATE
        }).getFullYear();

        return year;
    }

    function getTransactionDetail(objResult) {

        var resultArray = [];
        var key = "";
        var accountsDetailJson = {};
        var transactionType = objResult[0];
        var vendorId = objResult[3];
        var customerId = objResult[4];
        var entityId = objResult[5];

        var entidad = "",
            internalId = "";

        if (["VendBill", "VendCred", "CardChrg"].indexOf(transactionType) >= 0) {
            entidad = "vendor";
            internalId = vendorId;
        } else if (["CustInvc", "CustCred", "CustPymt"].indexOf(transactionType) >= 0) {
            entidad = "customer";
            if (customerId.length) {
                internalId = customerId;
            } else {
                internalId = getCustomerIdFromJob(objResult[6]);
            }
        } else if (transactionType == "Journal") {
            if (vendorId != "") {
                entidad = "vendor";
                internalId = vendorId;
            } else if (customerId != "") {
                entidad = "customer";
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
                            label: "Formula (Text)",
                        }),
                        search.createColumn({ name: "internalid", label: "Internal ID" }),
                    ],
                });
                var ObjEntity = entitySearchObj.run().getRange(0, 10);
                var columns = ObjEntity[0].columns;

                if (ObjEntity[0].getValue(columns[0]) == "Vendor") {
                    var entidad = "vendor";
                } else if (ObjEntity[0].getValue(columns[0]) == "CustJob") {
                    var entidad = "customer";
                } else if (ObjEntity[0].getValue(columns[0]) == "Employee") {
                    var entidad = "employee";
                }
                internalId = entityId;
            }
        } else if (transactionType == "Check") {
            var entitySearchObj = search.create({
                type: "entity",
                filters: [
                    ["internalid", "anyof", vendorId]
                ],
                columns: [
                    search.createColumn({
                        name: "formulatext",
                        formula: "{type.id}",
                        label: "Formula (Text)",
                    }),
                    search.createColumn({ name: "internalid", label: "Internal ID" }),
                ],
            });
            var ObjEntity = entitySearchObj.run().getRange(0, 10);
            var columns = ObjEntity[0].columns;
            // 0. Tipo de entidad
            if (ObjEntity[0].getValue(columns[0]) == "Vendor") {
                var entidad = "vendor";
            } else if (ObjEntity[0].getValue(columns[0]) == "CustJob") {
                var entidad = "customer";
            } else {
                var entidad = "employee";
            }
            internalId = vendorId;
        }

        // INFORMACION getInformation() 0-7
        if (entidad == "employee") {
            var aux_array = getInformationEmploy(entidad, internalId);
        } else {
            var aux_array = getInformation(entidad, internalId);
        }

        //! NOTA: el numero 800197268 es el NIT DE LA DIAN, A PEDIDO DEL CONTADOR, cuando se tiene ese nit y es de tipo journal, no se debe incluir en el reporte
        resultArray = (aux_array.indexOf("800197268") >= 0 && transactionType == "Journal") ? [] : resultArray.concat(aux_array);
        //log.debug("aux_array", aux_array);
        //&& transactionType == "Journal"
        if (resultArray.length != 0) {
            resultArray[8] = Number(objResult[1]);
            resultArray[9] = Number(objResult[2]);

            //key = resultArray[0] + "|" + resultArray[1] + "|" + resultArray[2] + "|" + resultArray[3] + "|" + resultArray[4] + "|" + resultArray[5] + "|" + resultArray[6] + "|" + resultArray[7];
            key = entidad + "|" + internalId;
            //log.debug('key',key);
            accountsDetailJson[key] = resultArray;
            //log.debug("resultArray ", resultArray);
            return accountsDetailJson;
        }
    }

    function getInformation(entidad, internalId) {
        var auxArray = [];
        if (entidad != "" && internalId != "") {
            var newSearch = search.create({
                type: entidad,
                filters: [
                    ["internalid", "is", internalId]
                ],
                columns: [
                    search.createColumn({
                        name: "formulatext",
                        formula: "{custentity_lmry_sunat_tipo_doc_cod}",
                        label: "0. Tipo de Documento",
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "{vatregnumber}",
                        label: "1. NIT",
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "CASE WHEN {custentity_lmry_sunat_tipo_doc_id} = 'NIT' THEN {custentity_lmry_digito_verificator} ELSE '' END",
                        label: "2. DV",
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "CASE WHEN {isperson} = 'T' THEN {lastname} ELSE '' END",
                        label: "3. Apellidos",
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "CASE WHEN {isperson} = 'T' THEN {firstname} ELSE '' END",
                        label: "4. Nombres",
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "CASE WHEN {isperson} = 'F' THEN {companyname} ELSE '' END",
                        label: "5. Razón Social",
                    }),
                ],
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
                if (objResult[0].getValue(columns[3]).split(" ")[0]) {
                    auxArray[3] = objResult[0].getValue(columns[3]).split(" ")[0];
                } else {
                    auxArray[3] = "";
                }

                // 4. Apellido Materno
                if (objResult[0].getValue(columns[3]).split(" ")[1]) {
                    auxArray[4] = objResult[0].getValue(columns[3]).split(" ")[1];
                } else {
                    auxArray[4] = "";
                }

                // 5. Primer Nombre
                if (objResult[0].getValue(columns[4]).split(" ")[0]) {
                    auxArray[5] = objResult[0].getValue(columns[4]).split(" ")[0];
                } else {
                    auxArray[5] = "";
                }

                // 6. Segundo Nombre
                if (objResult[0].getValue(columns[4]).split(" ")[1]) {
                    auxArray[6] = objResult[0].getValue(columns[4]).split(" ")[1];
                } else {
                    auxArray[6] = "";
                }

                // 7. Razón Social
                auxArray[7] = objResult[0].getValue(columns[5]);
            } else {
                log.debug("entidad y id sin macth id:", entidad + "-" + internalId);
            }
            return auxArray;
        } else {
            log.debug("entidad y id vacio", "vacio");
            return [];
        }
    }

    function getInformationEmploy(entityType, entityId) {
        var auxArray = [];

        if (entityType != "" && entityId != "") {
            var newSearch = search.create({
                type: entityType,
                filters: [
                    ["internalid", "is", entityId]
                ],
                columns: [
                    search.createColumn({
                        name: "formulatext",
                        formula: "{custentity_lmry_sunat_tipo_doc_cod}",
                        label: "0. Tipo de Documento",
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "{custentity_lmry_sv_taxpayer_number}",
                        label: "1. NIT",
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "CASE WHEN {custentity_lmry_sunat_tipo_doc_id} = 'NIT' THEN {custentity_lmry_digito_verificator} ELSE '' END",
                        label: "2. DV",
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "{lastname}",
                        label: "3. Apellidos",
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "{firstname}",
                        label: "4. Nombres",
                    }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "' '",
                        label: "5. Razón Social",
                    }),
                ],
            });

            var objResult = newSearch.run().getRange(0, 1000);
            if (objResult && objResult.length) {
                var columns = objResult[0].columns;

                // 0. Tipo de Documento
                auxArray[0] = objResult[0].getValue(columns[0]);

                // 1. NIT
                auxArray[1] = QuitarCaracteres(objResult[0].getValue(columns[1]));

                // 2. NID
                auxArray[2] = objResult[0].getValue(columns[2]);

                // 3. Apellido Paterno
                if (objResult[0].getValue(columns[3]).split(" ")[0]) {
                    auxArray[3] = objResult[0].getValue(columns[3]).split(" ")[0];
                } else {
                    auxArray[3] = "";
                }

                // 4. Apellido Materno
                if (objResult[0].getValue(columns[3]).split(" ")[1]) {
                    auxArray[4] = objResult[0].getValue(columns[3]).split(" ")[1];
                } else {
                    auxArray[4] = "";
                }

                // 5. Primer Nombre
                if (objResult[0].getValue(columns[4]).split(" ")[0]) {
                    auxArray[5] = objResult[0].getValue(columns[4]).split(" ")[0];
                } else {
                    auxArray[5] = "";
                }

                // 6. Segundo Nombre
                if (objResult[0].getValue(columns[4]).split(" ")[1]) {
                    auxArray[6] = objResult[0].getValue(columns[4]).split(" ")[1];
                } else {
                    auxArray[6] = "";
                }

                // 7. Razón Social
                auxArray[7] = objResult[0].getValue(columns[5]);
            } else {
                log.debug("entidad y id sin macth 2", entityType + "-" + entityId);
            }
        }
        return auxArray;
    }

    function QuitarCaracteres(str) {
        var nit = "";
        for (var i = 0; i < str.length; i++) {
            if (isInteger(Number(str[i])) && str[i] != " ") {
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

    function ObtenerCuentasFormatoMM() {
        var newSearch = search.create({
            type: "account",
            filters: [
                ["custrecord_lmry_co_puc_formatgy", "anyof", "3"]
            ],
            columns: [
                search.createColumn({
                    name: "formulanumeric",
                    formula: "{internalid}",
                    label: "0. Account Id",
                }),
            ],
        });

        var accountJson = {};
        var objResult = newSearch.run().getRange(0, 1000);
        var accountsIdArray = [];

        if (objResult && objResult.length) {
            var columns;
            for (var i = 0; i < objResult.length; i++) {
                columns = objResult[i].columns;
                accountsIdArray.push(objResult[i].getValue(columns[0]));
            }

            return accountsIdArray;
        } else {
            return [];
        }
    }

    function RetornaNumero(nid) {
        if (nid != null && nid != "") {
            return nid.replace(/(\.|\-)/g, "");
        }
        return "";
    }

    function RecortarCaracteres(valor, numero) {
        if (valor != null && valor.length > numero) {
            return valor.substring(0, numero);
        }
        return valor;
    }

    function NoData() {
        log.debug("no data", "no data");
        var usuario = runtime.getCurrentUser();

        var generarXml = false;

        if (paramIdReport) {
            var report = search.lookupFields({
                type: "customrecord_lmry_co_features",
                id: paramIdReport,
                columns: ["name"],
            });
            reportName = report.name;
        }

        if (isSubsidiariaFeature) {
            var subsidiaryRecord = search.lookupFields({
                type: search.Type.SUBSIDIARY,
                id: paramSubsidiaria,
                columns: ["legalname"],
            });
            var companyName = subsidiaryRecord.legalname;
        } else {
            var pageConfig = config.load({
                type: config.Type.COMPANY_INFORMATION,
            });
            var companyName = pageConfig.getValue("legalname");
        }

        if (isMultibookFeature) {
            var multibookName = search.lookupFields({
                type: search.Type.ACCOUNTING_BOOK,
                id: paramMultibook,
                columns: ["name"],
            }).name;
        }

        var employee = search.lookupFields({
            type: search.Type.EMPLOYEE,
            id: usuario.id,
            columns: ["firstname", "lastname"],
        });
        var usuarioName = employee.firstname + " " + employee.lastname;

        if (generarXml) {
            var generatorLog = record.create({
                type: "customrecord_lmry_co_rpt_generator_log",
            });
        } else {
            var generatorLog = record.load({
                type: "customrecord_lmry_co_rpt_generator_log",
                id: paramIdLog,
            });
        }

        //Nombre de Archivo
        generatorLog.setValue({
            fieldId: "custrecord_lmry_co_rg_name",
            value: "No existe informacion para los criterios seleccionados.",
        });

        //Nombre de Reporte
        generatorLog.setValue({
            fieldId: "custrecord_lmry_co_rg_transaction",
            value: reportName,
        });

        //Nombre de Subsidiaria
        generatorLog.setValue({
            fieldId: "custrecord_lmry_co_rg_subsidiary",
            value: companyName,
        });

        //Periodo
        generatorLog.setValue({
            fieldId: "custrecord_lmry_co_rg_postingperiod",
            value: paramPeriodo,
        });

        if (isMultibookFeature) {
            //Multibook
            generatorLog.setValue({
                fieldId: "custrecord_lmry_co_rg_multibook",
                value: multibookName,
            });
        }

        //Creado Por
        generatorLog.setValue({
            fieldId: "custrecord_lmry_co_rg_employee",
            value: usuarioName,
        });

        var recordId = generatorLog.save();
    }

    function SaveFile(extension, strArchivo, numeroEnvio) {
        var numeroEnvio = numeroEnvio;
        //log.debug("Entro al save File", 'wiiiii');
        var folderId = objContext.getParameter({
            name: "custscript_lmry_file_cabinet_rg_co",
        });

        var generarXml = false;

        if (isSubsidiariaFeature) {
            var subsidiaryRecord = search.lookupFields({
                type: search.Type.SUBSIDIARY,
                id: paramSubsidiaria,
                columns: ["legalname"],
            });
            var companyName = subsidiaryRecord.legalname;
        } else {
            var pageConfig = config.load({
                type: config.Type.COMPANY_INFORMATION,
            });
            var companyName = pageConfig.getValue("legalname");
        }

        // Almacena en la carpeta de Archivos Generados
        if (folderId != "" && folderId != null) {
            // Extension del archivo

            var fileName = Name_File(numeroEnvio) + extension;

            log.debug("fileName", fileName);

            // Crea el archivo
            var ventasXPagarFile;

            if (extension == ".xls") {
                //log.debug("strExcelVentasXPagar", strArchivo);
                ventasXPagarFile = file.create({
                    name: fileName,
                    fileType: file.Type.EXCEL,
                    contents: strArchivo,
                    folder: folderId,
                });
            } else {
                //log.debug("strXmlVentasXPagar", strArchivo);
                ventasXPagarFile = file.create({
                    name: fileName,
                    fileType: file.Type.XMLDOC,
                    contents: strArchivo,
                    folder: folderId,
                });

                generarXml = true;
            }

            var fileId = ventasXPagarFile.save();

            ventasXPagarFile = file.load({
                id: fileId,
            });

            var getURL = objContext.getParameter({
                name: "custscript_lmry_netsuite_location",
            });

            var fileUrl = "";

            if (getURL != "") {
                fileUrl += "https://" + getURL;
            }

            fileUrl += ventasXPagarFile.url;
            log.debug("fileUrl", fileUrl);

            if (fileId) {
                if (paramIdReport) {
                    var report = search.lookupFields({
                        type: "customrecord_lmry_co_features",
                        id: paramIdReport,
                        columns: ["name"],
                    });
                    reportName = report.name;
                }

                var usuario = runtime.getCurrentUser();
                var employee = search.lookupFields({
                    type: search.Type.EMPLOYEE,
                    id: usuario.id,
                    columns: ["firstname", "lastname"],
                });
                var usuarioName = employee.firstname + " " + employee.lastname;

                if (generarXml) {
                    var recordLog = record.create({
                        type: "customrecord_lmry_co_rpt_generator_log",
                    });
                } else {
                    var recordLog = record.load({
                        type: "customrecord_lmry_co_rpt_generator_log",
                        id: paramIdLog,
                    });
                }
                //Nombre de Archivo
                recordLog.setValue({
                    fieldId: "custrecord_lmry_co_rg_name",
                    value: fileName,
                });

                //Url de Archivo
                recordLog.setValue({
                    fieldId: "custrecord_lmry_co_rg_url_file",
                    value: fileUrl,
                });

                //Nombre de Reporte
                recordLog.setValue({
                    fieldId: "custrecord_lmry_co_rg_transaction",
                    value: reportName,
                });

                //Nombre de Subsidiaria
                recordLog.setValue({
                    fieldId: "custrecord_lmry_co_rg_subsidiary",
                    value: companyName,
                });

                //Periodo
                recordLog.setValue({
                    fieldId: "custrecord_lmry_co_rg_postingperiod",
                    value: paramPeriodo,
                });

                if (isMultibookFeature) {
                    //Multibook

                    var multibookName = search.lookupFields({
                        type: search.Type.ACCOUNTING_BOOK,
                        id: paramMultibook,
                        columns: ["name"],
                    }).name;

                    recordLog.setValue({
                        fieldId: "custrecord_lmry_co_rg_multibook",
                        value: multibookName,
                    });
                }

                //Creado Por
                recordLog.setValue({
                    fieldId: "custrecord_lmry_co_rg_employee",
                    value: usuarioName,
                });

                recordLog.save();
                //libreria.sendrptuser(reportName, 3, fileName);
                libreriaGeneral.sendConfirmUserEmail(reportName, 3, fileName, language);
            }
        } else {
            log.debug({
                title: "Creacion de File:",
                details: "No existe el folder",
            });
        }
    }

    function Name_File(numeroEnvio) {
        var name = "";

        name =
            "Dmuisca_" +
            completar_cero(2, paramConcepto) +
            "01005" +
            "71" +
            paramPeriodo +
            completar_cero(8, numeroEnvio);

        return name;
    }

    function completar_cero(long, valor) {
        if (("" + valor).length <= long) {
            if (long != ("" + valor).length) {
                for (var i = ("" + valor).length; i < long; i++) {
                    valor = "0" + valor;
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

    function ObtenerBook(booking) {
        if (booking != "") {
            var auxiliar = ("" + booking).split("&");
            var final = "";

            if (isMultibookFeature) {
                var id_libro = auxiliar[0].split("|");
                var exchange_rate = auxiliar[1].split("|");

                for (var i = 0; i < id_libro.length; i++) {
                    if (Number(id_libro[i]) == Number(paramMultibook)) {
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
        } else {
            return 1;
        }
    }

    /* ********************************************************************
     * Generacion de estructura excel y XML
     * ********************************************************************/
    function GenerarExcel(context, numeroEnvio) {
        if (language == "en") {
            var cabecera = [
                "FORM 1001: PAYMENTS OR CREDITS ON ACCOUNT AND WITHHOLDINGS MADE",
                "Company Name",
                "Tax Number",
                "Period",
                "Multibook",
                "Concept",
                "Type of Document",
                "NID",
                "1st Last Name",
                "2nd Last Name",
                "1st Name",
                "2nd Name",
                "Company Name",
                "Address",
                "Department",
                "Municipality",
                "Country",
                "Payment or credit to deductible account",
                "Payment or credit to NOT deductible account",
                "VAT greater value of the cost or deductible expense",
                "VAT greater value of the cost or non-deductible expense",
                "Ret. Source Pract. Income",
                "Ret. Source Assumed Income",
                "Ret. Source Pract. VAT",
                "Ret. Source Pract. Non-domiciled VAT",
            ];
        } else if (language == "pt") {
            var cabecera = [
                "FORMULARIO 1001: PAGAMENTOS OU CREDITOS POR CONTA E DEDUÇÕES EFECTUADAS",
                "Razão Social",
                "Numero de identificação fiscal",
                "Periodo",
                "Multibook",
                "Conceito",
                "Tipo de Documento",
                "NID",
                "1er Apelli",
                "2do Apelli",
                "1er Nombre",
                "2do Nombre",
                "Razão Social",
                "Endereço",
                "Departamento",
                "Municipio",
                "Pais",
                "Pagamento dedutivel ou credito em conta",
                "Pagamento ou credito por conta NÃO dedutível",
                "IVA valor mais elevado do custo ou da despesa dedutivel",
                "IVA valor mais elevado do custo ou despesa não dedutivel",
                "Ret. Fonte Pract. Rendimento",
                "Ret. Fonte Asumida Rendimento",
                "Ret. Fonte Pract. Iva Reg. Comum",
                "Ret. Fonte Pract. Iva não domiciliado",
            ];
        } else {
            var cabecera = [
                "FORMULARIO 1001: PAGOS O ABONOS EN CUENTA Y RETENCIONES PRACTICADAS",
                "Razon Social",
                "Numero de Impuesto",
                "Periodo",
                "Multibook",
                "Concepto",
                "Tipo de Documento",
                "NID",
                "1er Apelli",
                "2do Apelli",
                "1er Nombre",
                "2do Nombre",
                "Razon Social",
                "Direccion",
                "Departamento",
                "Municipio",
                "Pais",
                "Pago o abono en cuenta deducible",
                "Pago o abono en cuenta NO deducible",
                "Iva mayor valor del costo o gasto deducible",
                "Iva mayor valor del costo o gasto no deducible",
                "Ret. Fuente Pract. Renta",
                "Ret. Fuente Asumida Renta",
                "Ret. Fuente Pract. Iva Reg. Comun",
                "Ret. Fuente Pract. Iva No Domiciliados",
            ];
        }

        if (isSubsidiariaFeature) {
            var subsidiaryRecord = search.lookupFields({
                type: search.Type.SUBSIDIARY,
                id: paramSubsidiaria,
                columns: ["legalname", "taxidnum"],
            });
            var companyName = subsidiaryRecord.legalname;
            var companyRuc = subsidiaryRecord.taxidnum;
        } else {
            var pageConfig = config.load({
                type: config.Type.COMPANY_INFORMATION,
            });
            var companyName = pageConfig.getValue("legalname");
            var companyRuc = pageConfig.getValue("employerid");
        }

        if (isMultibookFeature) {
            var multibookName = search.lookupFields({
                type: search.Type.ACCOUNTING_BOOK,
                id: paramMultibook,
                columns: ["name"],
            }).name;
        }

        var periodStartDate = "01/01/" + paramPeriodo;

        var periodEndDate = "31/12/" + paramPeriodo;

        var cabecera = {
            tdoc: { en: "TDOC", pt: "TDOC", es: "TDOC" },
            nid: { en: "NID", pt: "NID", es: "NID" },
            dv: { en: "D.V.", pt: "D.V.", es: "D.V." },
            pApell: { en: "1st Last Name", pt: "1ro Sobrenome", es: "1er Apellido" },
            sApell: { en: "2nd Last Name", pt: "2do Sobrenome ", es: "2do Apellido" },
            pNom: { en: "1st Name", pt: "1ro Nome ", es: "1er Nombre" },
            sNom: { en: "2nd Name", pt: "2do Nome", es: "2do Nombre" },
            razonSocial: {
                en: "Company name",
                pt: "Razao Social",
                es: "Razon Social",
            },
            impDesc: {
                en: "Discounting Tax",
                pt: "Desconto de Imposto",
                es: "Impuesto Descontable",
            },
            ivaxdev: {
                en: "VAT x Refund",
                pt: "Iva x reembolso",
                es: "Iva x Devolucion",
            },
            titulo: {
                en: "FORM 1005: SALES TAX PAYABLE (DISCOUNTABLE)",
                pt: "FORMULARIO 1005: IMPOSTO DE VENDAS A PAGAR (DESCONTAVEL)",
                es: "FORMULARIO 1005: IMPUESTO A LAS VENTAS POR PAGAR (DESCONTABLE)",
            },
            taxnum: {
                en: "Tax Number",
                pt: "Numero de identificacao fiscal",
                es: "Numero de Impuesto",
            },
            periodo: { en: "Period", pt: "Periodo", es: "Periodo" },
            al: { en: " to ", pt: " al ", es: " al " },
            multibook: { en: "Multibook", pt: "Multibook", es: "Libro Contable" },
            origin: { en: "Origin", pt: "Origem", es: "Origen" },
            date: { en: "Date", pt: "Data", es: "Fecha" },
            time: { en: "Time", pt: "Hora", es: "Hora" },
        };

        //PDF Normalization
        var todays = parseDateTo(new Date(), "DATE");
        var currentTime = getTimeHardcoded(parseDateTo(new Date(), "DATETIME"));

        var xlsString = "";
        //cabecera de excel
        xlsString =
            '<?xml version="1.0" encoding="UTF-8" ?><?mso-application progid="Excel.Sheet"?>';
        xlsString +=
            '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" ';
        xlsString += 'xmlns:o="urn:schemas-microsoft-com:office:office" ';
        xlsString += 'xmlns:x="urn:schemas-microsoft-com:office:excel" ';
        xlsString += 'xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet" ';
        xlsString += 'xmlns:html="http://www.w3.org/TR/REC-html40">';
        xlsString += "<Styles>";
        xlsString +=
            '<Style ss:ID="s21"><Font ss:Bold="1" ss:Size="12" /><Alignment ss:Horizontal="Center" ss:Vertical="Bottom"/></Style>';
        xlsString +=
            '<Style ss:ID="s22"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom"/></Style>';
        xlsString +=
            '<Style ss:ID="s23"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom"/><NumberFormat ss:Format="_(* ###0_);_(* (###0);_(@_)"/></Style>';
        xlsString +=
            '<Style ss:ID="s24"><NumberFormat ss:Format="_(* ###0_);_(* (###0);_(@_)"/></Style>';
        xlsString += '</Styles><Worksheet ss:Name="Sheet1">';

        xlsString += "<Table>";
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

        //Cabecera
        xlsString += "<Row>";
        xlsString += "<Cell></Cell>";
        xlsString += "<Cell></Cell>";
        xlsString += "<Cell></Cell>";
        xlsString += "<Cell></Cell>";
        xlsString += "<Cell></Cell>";
        xlsString +=
            '<Cell ss:StyleID="s21"><Data ss:Type="String">' +
            cabecera.titulo[language] +
            " </Data></Cell>";
        xlsString += "</Row>";
        xlsString += "<Row></Row>";
        xlsString += "<Row>";
        xlsString += "<Cell></Cell>";
        xlsString += "<Cell></Cell>";
        xlsString += "<Cell></Cell>";
        xlsString +=
            '<Cell><Data ss:Type="String">' +
            cabecera.razonSocial[language] +
            ": " +
            companyName +
            "</Data></Cell>";
        xlsString += "</Row>";
        xlsString += "<Row>";
        xlsString += "<Cell></Cell>";
        xlsString += "<Cell></Cell>";
        xlsString += "<Cell></Cell>";
        xlsString +=
            '<Cell><Data ss:Type="String">' +
            cabecera.taxnum[language] +
            ": " +
            companyRuc +
            "</Data></Cell>";
        xlsString += "</Row>";
        xlsString += "<Row>";
        xlsString += "<Cell></Cell>";
        xlsString += "<Cell></Cell>";
        xlsString += "<Cell></Cell>";
        xlsString +=
            '<Cell><Data ss:Type="String">' +
            cabecera.periodo[language] +
            ": " +
            periodStartDate +
            cabecera.al[language] +
            periodEndDate +
            "</Data></Cell>";
        xlsString += "</Row>";
        if (isMultibookFeature) {
            xlsString += "<Row>";
            xlsString += "<Cell></Cell>";
            xlsString += "<Cell></Cell>";
            xlsString += "<Cell></Cell>";
            xlsString +=
                '<Cell><Data ss:Type="String">' + cabecera.multibook[language]
                + ": " + multibookName +
                "</Data></Cell>";
            xlsString += "</Row>";
        }

        // PDF Normalization
        xlsString += "<Row>";
        xlsString += "<Cell></Cell>";
        xlsString += "<Cell></Cell>";
        xlsString += "<Cell></Cell>";
        xlsString +=
            '<Cell><Data ss:Type="String">' + cabecera.origin[language] +
            ": Netsuite" +
            "</Data></Cell>";
        xlsString += "</Row>";

        xlsString += "<Row>";
        xlsString += "<Cell></Cell>";
        xlsString += "<Cell></Cell>";
        xlsString += "<Cell></Cell>";
        xlsString +=
            '<Cell><Data ss:Type="String">' + cabecera.date[language] +
            ": " + todays +
            "</Data></Cell>";
        xlsString += "</Row>";

        xlsString += "<Row>";
        xlsString += "<Cell></Cell>";
        xlsString += "<Cell></Cell>";
        xlsString += "<Cell></Cell>";
        xlsString +=
            '<Cell><Data ss:Type="String">' + cabecera.time[language] +
            ": " + currentTime +
            "</Data></Cell>";
        xlsString += "</Row>";

        // End PDF Normalization


        xlsString += "<Row></Row>";
        xlsString += "<Row></Row>";
        xlsString +=
            "<Row>" +
            '<Cell ss:StyleID="s21"><Data ss:Type="String">' +
            cabecera.tdoc[language] +
            "</Data></Cell>" +
            '<Cell ss:StyleID="s21"><Data ss:Type="String">' +
            cabecera.nid[language] +
            "</Data></Cell>" +
            '<Cell ss:StyleID="s21"><Data ss:Type="String">' +
            cabecera.dv[language] +
            "</Data></Cell>" +
            '<Cell ss:StyleID="s21"><Data ss:Type="String">' +
            cabecera.pApell[language] +
            "</Data></Cell>" +
            '<Cell ss:StyleID="s21"><Data ss:Type="String">' +
            cabecera.sApell[language] +
            "</Data></Cell>" +
            '<Cell ss:StyleID="s21"><Data ss:Type="String">' +
            cabecera.pNom[language] +
            "</Data></Cell>" +
            '<Cell ss:StyleID="s21"><Data ss:Type="String">' +
            cabecera.sNom[language] +
            "</Data></Cell>" +
            '<Cell ss:StyleID="s21"><Data ss:Type="String">' +
            cabecera.razonSocial[language] +
            "</Data></Cell>" +
            '<Cell ss:StyleID="s21"><Data ss:Type="String">' +
            cabecera.impDesc[language] +
            "</Data></Cell>" +
            '<Cell ss:StyleID="s21"><Data ss:Type="String">' +
            cabecera.ivaxdev[language] +
            "</Data></Cell>" +
            "</Row>";

        for (key in context) {
            //context.each(function(key, value) {
            var objResult = context[key];
            //log.debug('context', objResult);

            xlsString += "<Row>";

            // 0. TDOC
            if (
                objResult[0] != "" &&
                objResult[0] != null &&
                objResult[0] != "- None -"
            ) {
                xlsString +=
                    '<Cell><Data ss:Type="String">' + objResult[0] + "</Data></Cell>";
            } else {
                xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
            }
            // 1. NID
            if (
                objResult[1] != "" &&
                objResult[1] != null &&
                objResult[1] != "- None -"
            ) {
                xlsString +=
                    '<Cell><Data ss:Type="String">' + objResult[1] + "</Data></Cell>";
            } else {
                xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
            }
            // 2. D.V.
            if (
                objResult[2] != "" &&
                objResult[2] != null &&
                objResult[2] != "- None -"
            ) {
                xlsString +=
                    '<Cell><Data ss:Type="String">' + objResult[2] + "</Data></Cell>";
            } else {
                xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
            }
            // 3. 1ER APELL
            if (
                objResult[3] != "" &&
                objResult[3] != null &&
                objResult[3] != "- None -"
            ) {
                xlsString +=
                    '<Cell><Data ss:Type="String">' +
                    validarAcentos(objResult[3]) +
                    "</Data></Cell>";
            } else {
                xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
            }
            // 4. 2DO APLL
            if (
                objResult[4] != "" &&
                objResult[4] != null &&
                objResult[4] != "- None -"
            ) {
                xlsString +=
                    '<Cell><Data ss:Type="String">' +
                    validarAcentos(objResult[4]) +
                    "</Data></Cell>";
            } else {
                xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
            }
            // 5. 1ER NOMBRE
            if (
                objResult[5] != "" &&
                objResult[5] != null &&
                objResult[5] != "- None -"
            ) {
                xlsString +=
                    '<Cell><Data ss:Type="String">' +
                    validarAcentos(objResult[5]) +
                    "</Data></Cell>";
            } else {
                xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
            }
            // 6. 2DO NOMBRE
            if (
                objResult[6] != "" &&
                objResult[6] != null &&
                objResult[6] != "- None -"
            ) {
                xlsString +=
                    '<Cell><Data ss:Type="String">' +
                    validarAcentos(objResult[6]) +
                    "</Data></Cell>";
            } else {
                xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
            }
            // 7. RAZON SOCIAL
            if (
                objResult[7] != "" &&
                objResult[7] != null &&
                objResult[7] != "- None -"
            ) {
                xlsString +=
                    '<Cell><Data ss:Type="String">' +
                    validarAcentos(objResult[7]) +
                    "</Data></Cell>";
            } else {
                xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
            }
            // 8. IMPUESTO DESCONTABLE
            if (
                objResult[8] != "" &&
                objResult[8] != null &&
                objResult[8] != "- None -"
            ) {
                xlsString +=
                    '<Cell ss:StyleID="s24"><Data ss:Type="Number">' +
                    Number(objResult[8]).toFixed(0) +
                    "</Data></Cell>";
            } else {
                xlsString +=
                    '<Cell ss:StyleID="s24"><Data ss:Type="Number"></Data></Cell>';
            }
            // 9. IVA RECUPERADO EN DEVOLUCIONES EN COMPRAS ANULADAS, RESCINDIDAS O RESUELTAS
            if (
                objResult[9] != "" &&
                objResult[9] != null &&
                objResult[9] != "- None -"
            ) {
                xlsString +=
                    '<Cell ss:StyleID="s24"><Data ss:Type="Number">' +
                    Number(objResult[9]).toFixed(0) +
                    "</Data></Cell>";
            } else {
                xlsString +=
                    '<Cell ss:StyleID="s24"><Data ss:Type="Number"></Data></Cell>';
            }

            xlsString += "</Row>";
        }
        xlsString += "</Table></Worksheet></Workbook>";

        //log.debug("xlsString", xlsString);

        strExcelVentasXPagar = encode.convert({
            string: xlsString,
            inputEncoding: encode.Encoding.UTF_8,
            outputEncoding: encode.Encoding.BASE_64,
        });

        //log.debug("strExcelVentasXPagar", strExcelVentasXPagar);
        SaveFile(".xls", strExcelVentasXPagar, numeroEnvio);
    }

    function GenerarXml(context, numeroEnvio) {
        var xmlString = "";
        var strXmlVentasXPagar = "";
        var cantidadDatos = 0;
        var valorTotal = 0;
        var today = new Date();
        var anio = today.getFullYear();
        var mes = completar_cero(2, today.getMonth() + 1);
        var day = completar_cero(2, today.getDate());
        var hour = completar_cero(2, today.getHours());
        var min = completar_cero(2, today.getMinutes());
        var sec = completar_cero(2, today.getSeconds());
        today = anio + "-" + mes + "-" + day + "T" + hour + ":" + min + ":" + sec;

        for (key in context) {
            var objResult = context[key];


            xmlString += '<impventas ivade="' + Number(objResult[9]).toFixed(0);
            xmlString += '" vimp="' + Number(objResult[8]).toFixed(0);

            if (objResult[7]) {
                xmlString += '" raz="' + xml.escape(validarAcentos(objResult[7]));
            } else {
                xmlString += '" raz="';
            }

            if (objResult[6]) {
                xmlString += '" nom2="' + xml.escape(validarAcentos(objResult[6]));
            } else {
                xmlString += '" nom2="';
            }

            if (objResult[5]) {
                xmlString += '" nom1="' + xml.escape(validarAcentos(objResult[5]));
            } else {
                xmlString += '" nom1="';
            }

            if (objResult[4]) {
                xmlString +=
                    '" apl2="' + xml.escape(validarAcentos(objResult[4]));
            } else {
                xmlString += '" apl2="';
            }
            if (objResult[3]) {
                xmlString +=
                    '" apl1="' + xml.escape(validarAcentos(objResult[3]));
            } else {
                xmlString += '" apl1="';
            }

            xmlString +=
                '" dv="' +
                objResult[2] +
                '" nid="' +
                objResult[1] +
                '" tdoc="' +
                objResult[0];
            xmlString += '"/> \r\n';
            cantidadDatos++;
        }

        strXmlVentasXPagar += '<?xml version="1.0" encoding="ISO-8859-1"?> \r\n';
        strXmlVentasXPagar +=
            '<mas xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"> \r\n';
        strXmlVentasXPagar += "<Cab> \r\n";
        strXmlVentasXPagar += "<Ano>" + paramPeriodo + "</Ano> \r\n";
        strXmlVentasXPagar += "<CodCpt>" + paramConcepto + "</CodCpt> \r\n";
        strXmlVentasXPagar += "<Formato>1005</Formato> \r\n";
        strXmlVentasXPagar += "<Version>71</Version> \r\n";
        strXmlVentasXPagar += "<NumEnvio>" + numeroEnvio + "</NumEnvio> \r\n";
        strXmlVentasXPagar += "<FecEnvio>" + today + "</FecEnvio> \r\n";
        strXmlVentasXPagar +=
            "<FecInicial>" + paramPeriodo + "-01-01</FecInicial> \r\n";
        strXmlVentasXPagar +=
            "<FecFinal>" + paramPeriodo + "-12-31</FecFinal> \r\n";
        strXmlVentasXPagar += "<ValorTotal>" + valorTotal + "</ValorTotal> \r\n";
        strXmlVentasXPagar += "<CantReg>" + cantidadDatos + "</CantReg> \r\n";
        strXmlVentasXPagar += "</Cab>\r\n";
        strXmlVentasXPagar += xmlString;
        strXmlVentasXPagar += "</mas> \r\n";

        SaveFile(".xml", strXmlVentasXPagar, numeroEnvio);
    }

    function obtenerNumeroEnvio() {
        var numeroLote = 1;

        var savedSearch = search.create({
            type: "customrecord_lmry_co_lote_rpt_magnetic",
            filters: [
                search.createFilter({
                    name: "internalid",
                    join: "custrecord_lmry_co_id_magnetic_rpt",
                    operator: search.Operator.IS,
                    values: [paramIdFeatureByVersion],
                }),
                search.createFilter({
                    name: "internalid",
                    join: "custrecord_lmry_co_subsidiary",
                    operator: search.Operator.IS,
                    values: [paramSubsidiaria],
                }),
            ],
            columns: ["internalid", "custrecord_lmry_co_lote"],
        });
        var objResult = savedSearch.run().getRange(0, 1000);

        if (objResult == null || objResult.length == 0) {
            var loteXRptMgnRecord = record.create({
                type: "customrecord_lmry_co_lote_rpt_magnetic",
            });

            loteXRptMgnRecord.setValue({
                fieldId: "custrecord_lmry_co_id_magnetic_rpt",
                value: paramIdFeatureByVersion,
            });

            loteXRptMgnRecord.setValue({
                fieldId: "custrecord_lmry_co_year_issue",
                value: paramPeriodo,
            });

            loteXRptMgnRecord.setValue({
                fieldId: "custrecord_lmry_co_lote",
                value: numeroLote,
            });

            loteXRptMgnRecord.setValue({
                fieldId: "custrecord_lmry_co_subsidiary",
                value: paramSubsidiaria,
            });

            loteXRptMgnRecord.save();
        } else {
            var columns = objResult[0].columns;
            var internalId = objResult[0].getValue(columns[0]);
            numeroLote = Number(objResult[0].getValue(columns[1])) + 1;
            var loteXRptMgnRecord = record.load({
                type: "customrecord_lmry_co_lote_rpt_magnetic",
                id: internalId,
            });

            loteXRptMgnRecord.setValue({
                fieldId: "custrecord_lmry_co_lote",
                value: numeroLote,
            });

            loteXRptMgnRecord.save();
        }

        return numeroLote;
    }

    function validarAcentos(s) {
        var AccChars =
            "ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðòóôõöùúûüýÿ°–—ªº·";
        var RegChars =
            "SZszYAAAAAACEEEEIIIIDOOOOOUUUUYaaaaaaceeeeiiiidooooouuuuyyo--ao.";

        s = s.toString();
        for (var c = 0; c < s.length; c++) {
            for (var special = 0; special < AccChars.length; special++) {
                if (s.charAt(c) == AccChars.charAt(special)) {
                    s =
                        s.substring(0, c) +
                        RegChars.charAt(special) +
                        s.substring(c + 1, s.length);
                }
            }
        }
        return s;
    }

    function getJobId(transactionid) {

        var creditmemoSearchObj = search.create({
            type: "creditmemo",
            filters: [
                ["type", "anyof", "CustCred"],
                "AND", ["internalid", "anyof", transactionid],
                "AND", ["mainline", "is", "T"]
            ],
            columns: [
                search.createColumn({
                    name: "internalid",
                    join: "customer",
                    summary: "GROUP",
                    sort: search.Sort.ASC,
                    label: "Internal ID"
                })
            ]
        });

        var objResult = creditmemoSearchObj.run().getRange(0, 1000);
        if (objResult && objResult.length) {
            var columns = objResult[0].columns;
            // 0. Id job
            var JobId = objResult[0].getValue(columns[0]);
            log.debug('JobId', JobId);
            return JobId;
        } else {
            log.debug('no se encontro', 'job');
        }
    }

    function getCustomerIdFromJob(transactionid) {
        var jobid = getJobId(transactionid);

        var jobSearchObj = search.create({
            type: "job",
            filters: [
                ["internalid", "anyof", jobid]
            ],
            columns: [
                search.createColumn({
                    name: "internalid",
                    join: "customer",
                    label: "Internal ID"
                })
            ]
        });

        var objResult = jobSearchObj.run().getRange(0, 10);
        if (objResult && objResult.length) {
            var columns = objResult[0].columns;
            // 0. Id job
            var CustomerId = objResult[0].getValue(columns[0]);
            log.debug('CustomerId', CustomerId);
            return CustomerId;
        } else {
            log.debug('no se encontro', 'customer');
            return jobid;
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
        //reduce: reduce,
        summarize: summarize,
    };
});