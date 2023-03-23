/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for Inventory Balance Library                  ||
||                                                              ||
||  File Name: LMRY_CO_ReportGenerator_STLT_2.0.js              ||
||                                                              ||
||  Version Date         Author        Remarks                  ||
||  2.0     Aug 16 2018  LatamReady    Use Script 2.0           ||
 \= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.0
 * @NScriptType Suitelet
 * @NModuleScope Public
 */
define(["N/ui/serverWidget", "N/search", "N/runtime", "N/record", "N/redirect", "N/task", "N/log", "N/config", "/SuiteBundles/Bundle 37714/Latam_Library/LMRY_LibraryReport_LBRY_V2.js", 'N/format', require], runSuitelet);
var UI, SEARCH, RECORD, RUNTIME, REDIRECT, TASK, LOG, CONFIG, LIBFEATURE, REQUIRE;
// Titulo del Suitelet

var LMRY_script = "LMRY Report Generator CO STLT";
var namereport = "Latam Report Generator CO";
var language;

function runSuitelet(ui, search, runtime, record, redirect, task, log, config, libfeature, format, require) {

    UI = ui;
    SEARCH = search;
    RUNTIME = runtime;
    RECORD = record;
    REDIRECT = redirect;
    TASK = task;
    LOG = log;
    CONFIG = config;
    //LIBRARY = library;
    LIBFEATURE = libfeature;
    FORMAT = format;
    REQUIRE = require;
    LR_PermissionManager = null;
    var GLOBAL_LABELS = {};
    language = RUNTIME.getCurrentScript().getParameter({
        name: 'LANGUAGE'
    }).substring(0, 2);

    var returnObj = {};
    returnObj.onRequest = execute;
    return returnObj;
}

function getSecurityLibrary() {
    try {

        require(["/SuiteBundles/Bundle 35754/Latam_Security/LMRY_SECURITY_LICENSES_LBRY_V2.0"], function(library) {
            LR_PermissionManager = library;
        });
        LOG.error('SecurityLibrary.Bundle', 'Bundle 35754');

    } catch (err) {

        try {
            require(["/SuiteBundles/Bundle 37714/Latam_Security/LMRY_SECURITY_LICENSES_LBRY_V2.0"], function(library) {
                LR_PermissionManager = library;
            });

            LOG.error('SecurityLibrary.Bundle', 'Bundle 37714');
        } catch (err) {

            SEARCH.create({
                type: 'file',
                columns: ['internalid'],
                filters: [
                    ['name', 'is', 'LMRY_SECURITY_LICENSES_LBRY_V2.0.js']
                ]
            }).run().each(function(result) {
                require(['N/file'], function(file) {

                    var libraryPath = file.load(result.id).path;

                    require([libraryPath], function(library) {
                        LR_PermissionManager = library;
                    })

                });
                return false;
            })
            LOG.error('SecurityLibrary.Bundle', '- None -');
        }
    }

}

function execute(context) {

    var varMethod = context.request.method;
    try {

        getSecurityLibrary();

        var reportLicenseManager = LR_PermissionManager.open(LR_PermissionManager.Type.REPORT);

        var reponseLicense = reportLicenseManager.isCountryActived('CO');

        if (!reponseLicense.status) {

            LR_PermissionManager.createFormError(context, namereport, reponseLicense.error);

        } else {
            if (varMethod == 'GET') {
                GLOBAL_LABELS = getGlobalLabels();

                // Crea el folder
                search_folder();
                //Creacion de Folder
                var form = UI.createForm(namereport);

                var featuresubs = RUNTIME.isFeatureInEffect({
                    feature: "SUBSIDIARIES"
                });
                var featuremult = RUNTIME.isFeatureInEffect({
                    feature: "MULTIBOOK"
                });
                var FEATURE_CALENDAR = RUNTIME.isFeatureInEffect({
                    feature: 'MULTIPLECALENDARS'
                });

                /* ****** Grupo de Campos Criterios de Busqueda *******/
                form.addFieldGroup({
                    id: 'custpage_filran1',
                    label: GLOBAL_LABELS['tiposReporte'][language]
                });
                //Obtiene los datos de la lista de reportes SUNAT
                var fieldreports = form.addField({
                    id: 'custpage_lmry_reporte',
                    type: UI.FieldType.SELECT,
                    label: GLOBAL_LABELS['reporte'][language],
                    container: 'custpage_filran1'
                });

                var varFilter = new Array();
                varFilter[0] = SEARCH.createFilter({
                    name: 'isinactive',
                    operator: SEARCH.Operator.IS,
                    values: 'F'
                });
                var varRecord = SEARCH.create({
                    type: 'customrecord_lmry_co_features',
                    filters: varFilter,
                    columns: ['internalid', 'name']
                });
                var varResult = varRecord.run();
                var varRecordRpt = varResult.getRange({
                    start: 0,
                    end: 1000
                });

                if (varRecordRpt != null && varRecordRpt.length > 0) {
                    // Llena una linea vacia
                    fieldreports.addSelectOption({
                        value: 0,
                        text: ' '
                    });
                    // Llenado de listbox
                    for (var i = 0; i < varRecordRpt.length; i++) {
                        var reportID = varRecordRpt[i].getValue('internalid');
                        var reportNM = varRecordRpt[i].getValue('name');
                        fieldreports.addSelectOption({
                            value: reportID,
                            text: reportNM
                        });
                    }
                }
                fieldreports.isMandatory = true;

                /* ****** Grupo de Campos Criterios de Busqueda ****** */
                form.addFieldGroup({
                    id: 'custpage_filran2',
                    label: GLOBAL_LABELS['criteriosBusqueda'][language]
                });

                // Valida si es OneWorld
                if (featuresubs == true || featuresubs == 'T') //EN ALGUNAS INSTANCIAS DEVUELVE CADENA OTRAS DEVUELVE BOOLEAN
                {
                    var fieldsubs = form.addField({
                        id: 'custpage_subsidiary',
                        label: GLOBAL_LABELS['subsidiaria'][language],
                        type: UI.FieldType.SELECT,
                        container: 'custpage_filran2'
                    });

                    fieldsubs.isMandatory = true;

                    // Filtros
                    var Filter_Custo = new Array();
                    Filter_Custo[0] = SEARCH.createFilter({
                        name: 'isinactive',
                        operator: SEARCH.Operator.IS,
                        values: 'F'
                    });
                    Filter_Custo[1] = SEARCH.createFilter({
                        name: 'country',
                        operator: SEARCH.Operator.ANYOF,
                        values: 'CO'
                    });
                    var search_Subs = SEARCH.create({
                        type: SEARCH.Type.SUBSIDIARY,
                        filters: Filter_Custo,
                        columns: ['internalid', 'name']
                    });
                    var resul_sub = search_Subs.run();
                    var varRecordSub = resul_sub.getRange({
                        start: 0,
                        end: 1000
                    });

                    if (varRecordSub != null && varRecordSub.length > 0) {
                        // Llena una linea vacia
                        fieldsubs.addSelectOption({
                            value: 0,
                            text: ' '
                        });

                        // Llenado de listbox
                        for (var i = 0; i < varRecordSub.length; i++) {

                            var subID = varRecordSub[i].getValue('internalid');
                            var subNM = varRecordSub[i].getValue('name');
                            fieldsubs.addSelectOption({
                                value: subID,
                                text: subNM
                            });
                        }
                    }
                    //fieldsubs.isMandatory = true;
                }

                //agrego el campo Anio que si sirve para poder filtrar en busquedas -- Por Ivan Sep 2020 aun sigue la Pandemia
                var periodo_anual = form.addField({
                    id: 'custpage_anio_id',
                    label: 'Periodo Anual',
                    type: UI.FieldType.SELECT,
                    container: 'custpage_filran2'
                });

                var busqueda_periodos = SEARCH.create({
                    type: "accountingperiod",
                    filters: [
                        ["isadjust", "is", "F"],
                        "AND", ["isquarter", "is", "F"],
                        "AND", ["isinactive", "is", "F"],
                        "AND", ["isyear", "is", "T"],
                    ],
                    columns: [
                        SEARCH.createColumn({
                            name: "internalid",
                            label: "Internal ID"
                        }),
                        SEARCH.createColumn({
                            name: "periodname",
                            label: "Name"
                        }),
                        SEARCH.createColumn({
                            name: "formulatext",
                            formula: "TO_CHAR({startdate},'yyyy')",
                            label: "Formula (Text)"
                        })
                    ]
                });

                var resultado = busqueda_periodos.run().getRange(0, 1000);
                periodo_anual.addSelectOption({
                    value: '',
                    text: ''
                });
                if (resultado != null) {
                    for (var i = 0; i < resultado.length; i++) {
                        //console.log('aqui ta juan',resultado[i].getText('fiscalcalendar'))
                        LOG.error('VALUE', resultado[i].getValue('internalid'));
                        LOG.error('TEXT', resultado[i].getValue('formulatext'));
                        periodo_anual.addSelectOption({
                            value: resultado[i].getValue('internalid'),
                            text: resultado[i].getValue('formulatext')
                        });
                    }
                                      LOG.error("periodos",periodo_anual)

                }

                var unificado_1007 = form.addField({
                    id: 'custpage_unificado_1007',
                    label: 'Detallado CO 1007',
                    type: UI.FieldType.CHECKBOX,
                    container: 'custpage_filran2'
                });

                /**Adjustment */
                var Chec_adjusment = form.addField({
                    id: 'custpage_adjusment',
                    label: GLOBAL_LABELS['ajuste'][language],
                    type: UI.FieldType.CHECKBOX,
                    container: 'custpage_filran2'
                });
                /**Periodo Contable sin Periodos de Ajuste*/
                var periodo_mensual = form.addField({
                    id: 'custpage_custom_period',
                    label: GLOBAL_LABELS['periodo'][language],
                    type: UI.FieldType.SELECT,
                    container: 'custpage_filran2'
                });
                //periodo_mensual.isMandatory = true;

                var periodo_mensual_fin = form.addField({
                    id: 'custpage_custom_periodfin',
                    label: GLOBAL_LABELS['periodoFin'][language],
                    type: UI.FieldType.SELECT,
                    container: 'custpage_filran2'
                });

                //* CHECK DE AGRUPAMIENDO POR MESES DEL CERTIFICADO ACUMULADO */

                var checkMontAcum = form.addField({
                    id: 'custpage_grouping_by_months',
                    label: GLOBAL_LABELS['agrupadopormes'][language],
                    type: UI.FieldType.CHECKBOX,
                    container: 'custpage_filran2'
                });

                if (!(FEATURE_CALENDAR || FEATURE_CALENDAR == 'T')) {
                    log.debug('paso calendar');
                    var periodMensualSearch = SEARCH.create({
                        type: "accountingperiod",
                        filters: [
                            ["isadjust", "is", "F"],
                            "AND", ["isquarter", "is", "F"],
                            "AND", ["isinactive", "is", "F"],
                            "AND", ["isyear", "is", "F"],
                        ],
                        columns: [
                            SEARCH.createColumn({
                                name: "internalid",
                                label: "Internal ID"
                            }),
                            SEARCH.createColumn({
                                name: "periodname",
                                label: "Name"
                            }),
                            SEARCH.createColumn({
                                name: "startdate",
                                sort: SEARCH.Sort.ASC,
                                label: "Start Date"
                            })
                        ]
                    });

                    var resultado = periodMensualSearch.run().getRange(0, 1000);

                    periodo_mensual.addSelectOption({
                        value: '',
                        text: ''
                    });
                    periodo_mensual_fin.addSelectOption({
                        value: '',
                        text: ''
                    });

                    if (resultado != null) {
                        for (var i = 0; i < resultado.length; i++) {
                            periodo_mensual.addSelectOption({
                                value: resultado[i].getValue('internalid'),
                                text: resultado[i].getValue('periodname')
                            });

                            periodo_mensual_fin.addSelectOption({
                                value: resultado[i].getValue('internalid'),
                                text: resultado[i].getValue('periodname')
                            });

                        }
                    }

                }
                //*Filtro de criterio de formato para reportes
                var fieldTipoFormato = form.addField({
                    id: 'custpage_lmry_formato_tipo',
                    type: UI.FieldType.SELECT,
                    label: "Formato Reporte",
                    container: 'custpage_filran2'
                });

                fieldTipoFormato.addSelectOption({
                    value: 0,
                    text: "EXCEL"
                });
                fieldTipoFormato.addSelectOption({
                    value: 1,
                    text: "PDF"
                });


                var varGrupoEspecial = form.addFieldGroup({
                    id: 'custpage_filran3',
                    label: GLOBAL_LABELS['criteriosEspeciales'][language]
                });

                if (featuremult == true || featuremult == 'T') {
                    // variable - tipo - etiqueta - List/Record - grupo
                    var varFieldMultiB = form.addField({
                        id: 'custpage_multibook',
                        label: 'MULTIBOOK',
                        type: UI.FieldType.SELECT,
                        container: 'custpage_filran3'
                    });

                    varFieldMultiB.isMandatory = true;

                    var Filter_Custo = new Array();
                    var search_MultiB = SEARCH.create({
                        type: SEARCH.Type.ACCOUNTING_BOOK,
                        columns: ['internalid', 'name']
                    });

                    var resul_multib = search_MultiB.run();
                    var varRecordMultiB = resul_multib.getRange({
                        start: 0,
                        end: 1000
                    });
                    if (varRecordMultiB != null && varRecordMultiB.length > 0) {

                        // Llena una linea vacia
                        varFieldMultiB.addSelectOption({
                            value: 0,
                            text: ' '
                        });
                        // Llenado de listbox
                        for (var i = 0; i < varRecordMultiB.length; i++) {
                            var subID = varRecordMultiB[i].getValue('internalid');
                            var subNM = varRecordMultiB[i].getValue('name');
                            varFieldMultiB.addSelectOption({
                                value: subID,
                                text: subNM
                            });
                        }
                    }
                }

                /* ************************************************************
                 * Realiza busqueda por todos los campos agregados en la tabla
                 * de filtros de reportes
                 * ***********************************************************/
                var transacdata = SEARCH.load({
                    id: 'customsearch_lmry_co_filter_report'
                });
                var auxfield = '';

                ColIdFilter = SEARCH.createColumn({
                    name: 'custrecord_lmry_co_filter_id'
                });
                ColTypeFilter = SEARCH.createColumn({
                    name: 'custrecord_lmry_co_filter_field_type'
                });
                ColLabelFilter = SEARCH.createColumn({
                    name: 'custrecord_lmry_co_filter_field_label'
                });
                ColListFilter = SEARCH.createColumn({
                    name: 'custrecord_lmry_co_filter_list_record'
                });
                transacdata.colums = [ColIdFilter, ColTypeFilter, ColLabelFilter, ColListFilter];

                var resul_transac = transacdata.run();
                var varRecordTransac = resul_transac.getRange({
                    start: 0,
                    end: 1000
                });

                if (varRecordTransac != null && varRecordTransac.length > 0) {

                    for (var i = 0; i < varRecordTransac.length; i++) {
                        var idField = varRecordTransac[i].getValue('custrecord_lmry_co_filter_id');
                        var tipoField = varRecordTransac[i].getValue('custrecord_lmry_co_filter_field_type');
                        var lblField = varRecordTransac[i].getValue('custrecord_lmry_co_filter_field_label');
                        var listaRec = varRecordTransac[i].getValue('custrecord_lmry_co_filter_list_record');

                        if (listaRec == '') {
                            listaRec = null;
                        }
                        /* ************************************************************
                         * Agregando los campos, definidos en un registro personalizado
                         * varIdField       = ID Field
                         * tipoField    = Type
                         * lblField     = label
                         * listaRec     = List/Record
                         * ************************************************************/
                        if (auxfield != idField && idField != '' && idField != null && idField != 'custpage_locagroup' && idField != 'custpage_multibook') {
                            auxfield = idField;
                            if (idField == 'custpage_lmry_cr_vendor' || idField == 'custpage_tipo_retencion' || idField == 'custpage_lmry_cr_fechaini' || idField == 'custpage_lmry_cr_fechafin' || idField == 'custpage_digits' || idField == 'custpage_op_balance' /*agregar mas id si es que se quiere traducir rpt filters*/ ) {
                                var addFieldAux = form.addField({
                                    id: idField,
                                    label: GLOBAL_LABELS[idField][language],
                                    type: tipoField,
                                    source: listaRec,
                                    container: 'custpage_filran2'
                                });
                            } else {
                                var addFieldAux = form.addField({
                                    id: idField,
                                    label: lblField,
                                    type: tipoField,
                                    source: listaRec,
                                    container: 'custpage_filran2'
                                });
                            }

                            if (idField == 'custpage_msg') {
                                addFieldAux.updateDisplaySize({
                                    height: 10,
                                    width: 40
                                });
                            }

                        }
                    }
                }

                varGrupoEspecial.setShowBorder = true;

                // Mensaje para el cliente
                var strhtml = "<html>";
                strhtml += "<table border='0' class='table_fields' cellspacing='0' cellpadding='0'>" +
                    "<tr>" +
                    "</tr>" +
                    "<tr>" +
                    "<td class='text'>" +
                    "<div style=\"color: gray; font-size: 8pt; margin-top: 10px; padding: 5px; border-top: 1pt solid silver\">Important: By using the NetSuite Transaction, you assume all responsibility for determining whether the data you generate and download is accurate or sufficient for your purposes. You also assume all responsibility for the security of any data that you download from NetSuite and subsequently store outside of the NetSuite system.</div>" +
                    "</td>" +
                    "</tr>" +
                    "</table>" +
                    "</html>";

                var varInlineHtml = form.addField({
                    id: 'custpage_btn',
                    type: UI.FieldType.INLINEHTML,
                    label: 'custpage_lmry_v1_message'
                }).updateLayoutType({
                    layoutType: UI.FieldLayoutType.OUTSIDEBELOW
                }).updateBreakType({
                    breakType: UI.FieldBreakType.STARTCOL
                }).defaultValue = strhtml;

                var tab = form.addTab({
                    id: 'custpage_maintab',
                    label: 'Tab'
                });


                //sublista
                var listaLog = form.addSublist({
                    id: 'custpage_sublista',
                    type: UI.SublistType.STATICLIST,
                    label: GLOBAL_LABELS['logGeneracion'][language]
                });
                listaLog.addField({
                    id: 'custpage_lmry_rg_trandate',
                    label: GLOBAL_LABELS['fechaCreacion'][language],
                    type: UI.FieldType.TEXT
                });
                listaLog.addField({
                    id: 'custpage_lmry_rg_transaction',
                    label: GLOBAL_LABELS['informe'][language],
                    type: UI.FieldType.TEXT
                });
                listaLog.addField({
                    id: 'custpage_lmry_rg_postingperiod',
                    label: GLOBAL_LABELS['periodoLog'][language],
                    type: UI.FieldType.TEXT
                });
                listaLog.addField({
                    id: 'custpage_lmry_rg_subsidiary',
                    label: GLOBAL_LABELS['subsidiariaLog'][language],
                    type: UI.FieldType.TEXT
                });

                /* ************************************************************
                 * 2018/04/18 Verifica si esta activo la funcionalidad
                 *  MULTI-BOOK ACCOUNTING - ID multibook
                 * ***********************************************************/
                if (featuremult == true || featuremult == 'T') {
                    listaLog.addField({
                        id: 'custpage_lmry_rg_multibook',
                        label: 'Multi Book',
                        type: UI.FieldType.TEXT
                    });
                }
                listaLog.addField({
                    id: 'custpage_lmry_rg_employee',
                    label: GLOBAL_LABELS['creadoPor'][language],
                    type: UI.FieldType.TEXT
                });
                listaLog.addField({
                    id: 'custpage_lmry_rg_nombre',
                    label: GLOBAL_LABELS['nombreArchivo'][language],
                    type: UI.FieldType.TEXT
                });
                listaLog.addField({
                    id: 'custpage_lmry_rg_archivo',
                    label: GLOBAL_LABELS['descargar'][language],
                    type: UI.FieldType.TEXT
                });
                listaLog.addRefreshButton();

                var varLogData = SEARCH.load({
                    id: 'customsearch_lmry_co_rpt_generator_log'
                });
                var resul_LogData = varLogData.run();
                var varRecordLog = resul_LogData.getRange({
                    start: 0,
                    end: 1000
                });

                LOG.debug({
                    title: 'longitud: ',
                    details: varRecordLog.length
                });

                for (var i = 0; varRecordLog != null && i < varRecordLog.length; i++) {
                    //  var row = i + 1;
                    searchresult = varRecordLog[i];
                    var linktext = '';
                    var url = searchresult.getValue('custrecord_lmry_co_rg_url_file');

                    if (url != null && url != '') {
                        linktext = '<a target="_blank" href="' + searchresult.getValue('custrecord_lmry_co_rg_url_file') + '"download>Descarga</a>';
                    }

                    var creat = searchresult.getValue('created');

                    if (creat != null && creat != '') {
                        listaLog.setSublistValue({
                            id: 'custpage_lmry_rg_trandate',
                            line: i,
                            value: creat
                        });
                    }

                    var transact = searchresult.getValue('custrecord_lmry_co_rg_transaction');
                    if (transact != null && transact != '') {
                        listaLog.setSublistValue({
                            id: 'custpage_lmry_rg_transaction',
                            line: i,
                            value: transact
                        });
                    }

                    var periodname = searchresult.getValue('custrecord_lmry_co_rg_postingperiod');
                    if (periodname != null && periodname != '') {
                        listaLog.setSublistValue({
                            id: 'custpage_lmry_rg_postingperiod',
                            line: i,
                            value: periodname
                        });
                    }

                    var subsi = searchresult.getValue('custrecord_lmry_co_rg_subsidiary');
                    if (subsi != null && subsi != '') {
                        listaLog.setSublistValue({
                            id: 'custpage_lmry_rg_subsidiary',
                            line: i,
                            value: subsi
                        });
                    }

                    if (featuremult == true || featuremult == 'T') {

                        var mult = searchresult.getValue('custrecord_lmry_co_rg_multibook');
                        if (mult != null && mult != '') {
                            listaLog.setSublistValue({
                                id: 'custpage_lmry_rg_multibook',
                                line: i,
                                value: mult
                            });
                        }
                    }

                    var empleado = searchresult.getValue('custrecord_lmry_co_rg_employee');
                    if (empleado != null && empleado != '') {
                        listaLog.setSublistValue({
                            id: 'custpage_lmry_rg_employee',
                            line: i,
                            value: empleado
                        });
                    }

                    var nomb = searchresult.getValue('custrecord_lmry_co_rg_name');
                    if (nomb != null && nomb != '') {
                        listaLog.setSublistValue({
                            id: 'custpage_lmry_rg_nombre',
                            line: i,
                            value: nomb
                        });
                    }

                    if (linktext != '') {
                        listaLog.setSublistValue({
                            id: 'custpage_lmry_rg_archivo',
                            line: i,
                            value: linktext
                        });
                    }
                }

                var codigoConcepto = form.getField({
                    id: 'custpage_concept'
                });
                codigoConcepto.addSelectOption({
                    value: 1,
                    text: 'Insercion'
                });
                codigoConcepto.addSelectOption({
                    value: 2,
                    text: 'Reemplazo'
                });

                var entityTypeField = form.getField({
                    id: 'custpage_entity_type'
                });

                entityTypeField.addSelectOption({
                    value: 0,
                    text: ''
                });
                entityTypeField.addSelectOption({
                    value: 1,
                    text: 'Customer'
                });
                entityTypeField.addSelectOption({
                    value: 2,
                    text: 'Vendor'
                });
                entityTypeField.addSelectOption({
                    value: 3,
                    text: 'Employee'
                });
                entityTypeField.addSelectOption({
                    value: 4,
                    text: 'Other Name'
                });

                // Botones del formulario
                form.addSubmitButton(GLOBAL_LABELS['btnGenerar'][language]);
                form.addResetButton(GLOBAL_LABELS['btnCancelar'][language]);
                //Llama al cliente
                form.clientScriptModulePath = './LMRY_CO_ReportGenerator_CLNT_2.0.js';
                context.response.writePage(form);
            }

            if (varMethod == 'POST') {
                LOG.error('ANIO POST', context.request.parameters.custpage_anio_id);
                //Valida si es OneWorld
                var featuresubs = RUNTIME.isFeatureInEffect({
                    feature: "SUBSIDIARIES"
                });
                var featuremult = RUNTIME.isFeatureInEffect({
                    feature: "MULTIBOOK"
                });
                /*********************************************
                 * Regista en el log de generacion de archivos
                 ********************************************/
                var idrpts = context.request.parameters.custpage_lmry_reporte;
                var varReport = SEARCH.lookupFields({
                    type: 'customrecord_lmry_co_features',
                    id: idrpts,
                    columns: ['custrecord_lmry_co_id_schedule', 'custrecord_lmry_co_id_deploy', 'name']
                });

                // El arr_id[2] es el id del record feature by version
                var arr_id = ['', '', ''];

                if (idrpts == 35 || idrpts == 34 || idrpts == 29 || idrpts == 38 || idrpts == 40 || idrpts == 36 || idrpts == 37 || idrpts == 39) {
                    arr_id = ['', '', ''];
                    arr_id = busquedaVersion(idrpts, context);
                    log.debug('arr_id', arr_id);
                    varReport.custrecord_lmry_co_id_schedule = arr_id[0];
                    varReport.custrecord_lmry_co_id_deploy = arr_id[1];

                }

                var TituloInforme = varReport.name;
                var varIdSchedule = varReport.custrecord_lmry_co_id_schedule;
                var varIdDeploy = varReport.custrecord_lmry_co_id_deploy;
                LOG.error('varIdSchedule', varIdSchedule);
                LOG.error('varIdDeploy', varIdDeploy);

                var periodName = '';
                if (idrpts == 35 || idrpts == 34 || idrpts == 29 || idrpts == 38 || idrpts == 40 || idrpts == 36 || idrpts == 37 || idrpts == 39) {
                    periodName = context.request.parameters.custpage_txtanio;
                } else if (idrpts == 54 || idrpts == 53 || idrpts == 55 || idrpts == 52) {
                    //periodName = context.request.parameters.custpage_anio_id;
                    LOG.error('context.request.parameters.custpage_anio_id', context.request.parameters.custpage_anio_id);
                    if (context.request.parameters.custpage_anio_id) {
                        var licenses = LIBFEATURE.getLicenses(context.request.parameters.custpage_subsidiary);
                        var featureSpecialPeriod = LIBFEATURE.getAuthorization(677, licenses);
                        if (featureSpecialPeriod || featureSpecialPeriod == 'T') {
                            periodName = context.request.parameters.custpage_anio_id;
                        }
                        else {
                            var varPeriodo = SEARCH.lookupFields({
                                type: 'accountingperiod',
                                id: context.request.parameters.custpage_anio_id,
                                columns: ['periodname']
                            });
                            //Period Name
                            periodName = varPeriodo.periodname;
                        }
                    }

                } else if (idrpts == 43 || idrpts == 50 || idrpts == 23 || idrpts == 24 || idrpts == 25 || idrpts == 51 || idrpts == 57 || idrpts == 58) {
                    var varPeriodo = SEARCH.lookupFields({
                        type: 'accountingperiod',
                        id: context.request.parameters.custpage_custom_period,
                        columns: ['periodname']
                    });
                    //Period Name
                    periodName = varPeriodo.periodname;

                } else if (idrpts == 56) {
                    log.error('context.request.parameters.custpage_subsidiary', context.request.parameters.custpage_subsidiary);
                    var licenses = LIBFEATURE.getLicenses(context.request.parameters.custpage_subsidiary);
                    var featureSpecialPeriod = LIBFEATURE.getAuthorization(677, licenses);
                    if (featureSpecialPeriod || featureSpecialPeriod == 'T') {
                        periodName = context.request.parameters.custpage_lmry_cr_anio;
                        log.error('Special period', periodName);
                    } else {
                        var varPeriodo = SEARCH.lookupFields({
                            type: 'accountingperiod',
                            id: context.request.parameters.custpage_lmry_cr_anio,
                            columns: ['periodname']
                        });
                        //Period Name
                        periodName = varPeriodo.periodname;
                    }
                } else {
                    //Period enddate
                    if (context.request.parameters.custpage_periodo) {
                        var varPeriodo = SEARCH.lookupFields({
                            type: 'accountingperiod',
                            id: context.request.parameters.custpage_periodo,
                            columns: ['periodname']
                        });
                        //Period Name
                        periodName = varPeriodo.periodname;
                    }
                }

                log.debug('PERIOD NAME', periodName);

                // Creacion de la linea en el log de errores
                var varLogRecord = RECORD.create({
                    type: 'customrecord_lmry_co_rpt_generator_log'
                });

                varLogRecord.setValue('custrecord_lmry_co_rg_name', 'Pendiente');
                varLogRecord.setValue('custrecord_lmry_co_rg_transaction', TituloInforme);
                varLogRecord.setValue('custrecord_lmry_co_rg_postingperiod', periodName);

                if (idrpts == 26 || idrpts == 42) {
                    var fecha_format = context.request.parameters.custpage_lmry_cr_fechaini;
                    var aux_fecha_per = FORMAT.parse({
                        value: fecha_format,
                        type: FORMAT.Type.DATE
                    });
                    var MM = aux_fecha_per.getMonth() + 1;
                    var YYYY = aux_fecha_per.getFullYear();
                    var DD = aux_fecha_per.getDate();

                    if (('' + MM).length == 1) {
                        MM = '0' + MM;
                    }
                    fecha_format = DD + '/' + MM + '/' + YYYY;
                    var auxiliar = fecha_format.split('/');
                    fecha_format = TraePeriodo(auxiliar[1]) + ' ' + auxiliar[2];

                    varLogRecord.setValue('custrecord_lmry_co_rg_postingperiod', fecha_format);

                }

                if (featuresubs == true || featuresubs == 'T') {
                    // Trae el nombre de la subsidiaria
                    var varSubsidiary = SEARCH.lookupFields({
                        type: 'subsidiary',
                        id: context.request.parameters.custpage_subsidiary,
                        columns: ['legalname']
                    });

                    varLogRecord.setValue('custrecord_lmry_co_rg_subsidiary', varSubsidiary.legalname);

                } else {
                    var varCompanyReference = CONFIG.load({
                        type: CONFIG.Type.COMPANY_INFORMATION
                    });
                    companyname = varCompanyReference.getValue('legalname');
                    varLogRecord.setValue('custrecord_lmry_co_rg_subsidiary', companyname);

                }

                if (featuremult == true || featuremult == 'T') {
                    // Descripcion del MultiBook
                    var varIdBook = context.request.parameters.custpage_multibook;
                    var varMultiBook = SEARCH.lookupFields({
                        type: 'accountingbook',
                        id: varIdBook,
                        columns: ['name']
                    });
                    varLogRecord.setValue('custrecord_lmry_co_rg_multibook', varMultiBook.name);
                }

                var objUser = RUNTIME.getCurrentUser();

                varLogRecord.setValue('custrecord_lmry_co_rg_url_file', '');
                var varEmployee = SEARCH.lookupFields({
                    type: 'employee',
                    id: objUser.id,
                    columns: ['firstname', 'lastname']
                });
                var varEmployeeName = varEmployee.firstname + ' ' + varEmployee.lastname;
                varLogRecord.setValue('custrecord_lmry_co_rg_employee', varEmployeeName);

                var rec_id = varLogRecord.save();
                //Entidad del Reporte Balance Comprobacion por Terceros
                var entity_id;
                if (idrpts == 41 || idrpts == 50) {
                    //entity_id = request.getParameter('custpage_entity');
                    //custpage_customer
                    //custpage_vendor
                    //custpage_employee
                    switch (context.request.parameters.custpage_entity_type) {
                        case '1':
                            entity_id = context.request.parameters.custpage_customer;
                            break;
                        case '2':
                            entity_id = context.request.parameters.custpage_vendor;
                            break;
                        case '3':
                            entity_id = context.request.parameters.custpage_employee;
                            break;
                        case '4':
                            entity_id = context.request.parameters.custpage_othername;
                            break;
                    }

                }
                /*********************************************
                 * Pasa los parametros para los reportes
                 ********************************************/
                var params = {};

                LOG.debug('idrpts', idrpts);
                LOG.error('idrpts', idrpts);

                if (idrpts == 50) {
                    LOG.debug('Reporte Libro Balance de Comprobacion por Terceros', 'reporte Libro Balance de Comprobacion por Terceros');
                    // Reporte Libro Balance de Comprobacion por Terceros
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_terceros_mprdc_subsi'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_terceros_mprdc_period'] = context.request.parameters.custpage_custom_period;
                    params['custscript_lmry_terceros_mprdc_periodfin'] = context.request.parameters.custpage_custom_periodfin;
                    params['custscript_lmry_terceros_mprdc_record'] = rec_id;
                    params['custscript_lmry_terceros_mprdc_entity'] = entity_id;
                    params['custscript_lmry_terceros_mprdc_adjust'] = context.request.parameters.custpage_adjusment;
                    params['custscript_lmry_terceros_mprdc_opbalance'] = context.request.parameters.custpage_op_balance;
                    params['custscript_lmry_terceros_mprdc_8d'] = context.request.parameters.custpage_digits;

                    /* ***********************************************************************************
                     * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/

                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_terceros_mprdc_multi'] = context.request.parameters.custpage_multibook;
                    }
                }

                if (idrpts == 26) {
                    /* *************************************************************************************************************************
                     * 2018/10/29 Reporte Certificado de Retencion script 2.0
                     * ************************************************************************************************************************/
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_co_subsi_withbook_ret'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_co_periodini_withbook_re'] = context.request.parameters.custpage_lmry_cr_fechaini;
                    params['custscript_lmry_co_periodfin_withbook_re'] = context.request.parameters.custpage_lmry_cr_fechafin;
                    params['custscript_lmry_co_vendor_withbook_ret'] = context.request.parameters.custpage_lmry_cr_vendor;
                    params['custscript_lmry_co_type_withbook_ret'] = context.request.parameters.custpage_tipo_retencion;
                    params['custscript_lmry_co_idrpt_withbook_ret'] = rec_id;
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_co_multibook_withbook_re'] = context.request.parameters.custpage_multibook;
                    }

                }

                if (idrpts == 56) {
                    /* *************************************************************************************************************************
                     * 2018/10/29 Reporte Certificado de Retencion Acumuladas script 2.0
                     * ************************************************************************************************************************/
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_co_subsi_withbk_ret_acum'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_co_par_anio_wtbk_ret_ac'] = context.request.parameters.custpage_lmry_cr_anio;
                    params['custscript_lmry_co_vendor_withbk_ret_ac'] = context.request.parameters.custpage_lmry_cr_vendor;
                    params['custscript_lmry_co_type_withbk_ret_acum'] = context.request.parameters.custpage_tipo_retencion;
                    params['custscript_lmry_co_idrpt_wtbk_ret_acumul'] = rec_id;
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_co_multibook_wtbk_ret_ac'] = context.request.parameters.custpage_multibook;
                    }
                    params['custscript_lmry_co_group_month'] = context.request.parameters.custpage_grouping_by_months;
                }




                if (idrpts == 42) {

                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_co_subsi_withbook'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_co_periodini_withbook'] = context.request.parameters.custpage_lmry_cr_fechaini;
                    params['custscript_lmry_co_periodfin_withbook'] = context.request.parameters.custpage_lmry_cr_fechafin;
                    params['custscript_lmry_co_vendor_withbook'] = context.request.parameters.custpage_lmry_cr_vendor;
                    params['custscript_lmry_co_type_withbook'] = context.request.parameters.custpage_tipo_retencion;
                    params['custscript_lmry_co_idrpt_withbook'] = rec_id;
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_co_multibook_withbook'] = context.request.parameters.custpage_multibook;
                    }
                }

                if (idrpts == 22) {
                    // Reporte Libro de Inventario y Balance v2.0
                    params['custscript_lmry_periodo_invbalanc'] = context.request.parameters.custpage_periodo;
                    params['custscript_lmry_idrpt_invbalanc'] = rec_id;
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_subsidi_invbalanc'] = context.request.parameters.custpage_subsidiary;
                    }
                    /* ***********************************************************************************
                     * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_booking_invbalanc'] = context.request.parameters.custpage_multibook;
                    }
                }

                if (idrpts == 23) {
                    // Reporte Libro Diario v2.0
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_co_librodm_subsi'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_co_librodm_period'] = context.request.parameters.custpage_custom_period;
                    params['custscript_lmry_co_librodm_idreport'] = rec_id;
                    /* ***********************************************************************************
                     * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_co_librodm_multi'] = context.request.parameters.custpage_multibook;
                    }
                    params['custscript_lmry_co_adjust_diario'] = context.request.parameters.custpage_adjusment;
                    params['custscript_lmry_co_librodm_formatoreport'] = context.request.parameters.custpage_lmry_formato_tipo;

                }

                if (idrpts == 57) {
                    // Reporte Libro Diario Detallado v2.0
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_co_libdiariodet_subsi'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_co_libdiariodet_period'] = context.request.parameters.custpage_custom_period;
                    params['custscript_lmry_co_libdiariodet_idlog'] = rec_id;
                    /* ***********************************************************************************
                     * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_co_libdiariodet_multi'] = context.request.parameters.custpage_multibook;
                    }
                    params['custscript_lmry_co_libdiariodet_adjust'] = context.request.parameters.custpage_adjusment;
                    params['custscript_lmry_co_libdiariodet_repform'] = context.request.parameters.custpage_lmry_formato_tipo;

                }

                if (idrpts == 43) {
                    LOG.error('context.request.parameters.custpage_digits', context.request.parameters.custpage_digits);
                    var paramDigits = context.request.parameters.custpage_digits;
                    if (paramDigits == 'T' || paramDigits == true) {
                        paramDigits = 3;
                    } else {
                        paramDigits = 2;
                    }
                    params['custscript_lmry_invbal_digits'] = paramDigits;
                }
                //reporte de ARTICULO 4
                if (idrpts == 52) {
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_co_art4_subsidiaria'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_co_art4_periodo'] = context.request.parameters.custpage_periodo;
                    params['custscript_lmry_co_art4_recordid'] = rec_id;
                    params['custscript_lmry_co_art4_periodo_anual'] = context.request.parameters.custpage_anio_id;
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_co_art4_multibook'] = context.request.parameters.custpage_multibook;
                    }
                    params['custscript_lmry_co_art4_feature'] = idrpts;
                    params['custscript_lmry_co_art4_cabecera'] = context.request.parameters.custpage_insert_head;
                }

                //reporte de ARTICULO 1
                if (idrpts == 53) {
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_co_art1_subsidiaria'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_co_art1_periodo'] = context.request.parameters.custpage_periodo;
                    params['custscript_lmry_co_art1_recordid'] = rec_id;
                    params['custscript_lmry_co_art1_periodo_anual'] = context.request.parameters.custpage_anio_id;

                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_co_art1_multibook'] = context.request.parameters.custpage_multibook;
                    }
                    params['custscript_lmry_co_art1_feature'] = idrpts;
                    params['custscript_lmry_co_art1_cabecera'] = context.request.parameters.custpage_insert_head;
                }

                //REPORTE CO-  ARTICULO 6
                if (idrpts == 54) {
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_co_art6_subsi'] = context.request.parameters.custpage_subsidiary;
                    }
                    LOG.error('anio', context.request.parameters.custpage_anio_id);
                    params['custscript_lmry_co_art6_period'] = context.request.parameters.custpage_periodo;
                    params['custscript_lmry_co_art6_recid'] = rec_id;
                    params['custscript_lmry_co_art6_anual'] = context.request.parameters.custpage_anio_id;

                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_co_art6_mutibook'] = context.request.parameters.custpage_multibook;
                    }
                    params['custscript_lmry_co_art6_featid'] = idrpts;
                    params['custscript_lmry_co_art6_inserthead'] = context.request.parameters.custpage_insert_head;
                }

                //REPORTE CO-  ARTICULO 2
                if (idrpts == 55) {
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_co_art2_subsi'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_co_art2_period'] = context.request.parameters.custpage_periodo;
                    params['custscript_lmry_co_art2_recid'] = rec_id;
                    params['custscript_lmry_co_art2_anual'] = context.request.parameters.custpage_anio_id;
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_co_art2_mutibook'] = context.request.parameters.custpage_multibook;
                    }
                    params['custscript_lmry_co_art2_featid'] = idrpts;
                    params['custscript_lmry_co_art2_inserthead'] = context.request.parameters.custpage_insert_head;
                }

                if (idrpts == 24) {
                    // Reporte Libro Diario sin cierre v2.0
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_co_librod_subsi_sc'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_co_librod_period_sc'] = context.request.parameters.custpage_custom_period;
                    params['custscript_lmry_co_librod_idreport_sc'] = rec_id;
                    /* ***********************************************************************************
                     * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_co_librod_multibook_sc'] = context.request.parameters.custpage_multibook;
                    }
                    params['custscript_lmry_co_adjust_diario_sc'] = context.request.parameters.custpage_adjusment;
                }

                if (idrpts == 25) {
                    // Reporte Libro Mayor y Balance v2.0
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_co_maybalance_subsidiary'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_co_maybalance_period'] = context.request.parameters.custpage_custom_period;
                    params['custscript_lmry_co_maybalance_idreport'] = rec_id;
                    params['custscript_lmry_co_maybalance_adjust'] = context.request.parameters.custpage_adjusment;
                    params['custscript_lmry_co_maybalance_digits'] = context.request.parameters.custpage_digits;
                    /* ***********************************************************************************
                     * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_co_maybalance_multibook'] = context.request.parameters.custpage_multibook;
                    }
                }

                if (idrpts == 58) {
                    // Reporte Libro Mayor y Balance Detallado v2.0
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_co_maybaldet_subsidiary'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_co_maybaldet_period'] = context.request.parameters.custpage_custom_period;
                    params['custscript_lmry_co_maybaldet_idlog'] = rec_id;
                    params['custscript_lmry_co_maybaldet_adjust'] = context.request.parameters.custpage_adjusment;
                    params['custscript_lmry_co_maybaldet_digits'] = context.request.parameters.custpage_digits;
                    /* ***********************************************************************************
                     * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_co_maybaldet_multibook'] = context.request.parameters.custpage_multibook;
                    }
                }

                if (idrpts == 51) {
                    // Reporte Libro Mayor y Balance Anual
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_co_may_bal_anu_subsi'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_co_may_bal_anu_period'] = context.request.parameters.custpage_custom_period;
                    params['custscript_lmry_co_may_bal_anu_record'] = rec_id;
                    params['custscript_lmry_co_may_bal_anu_adjust'] = context.request.parameters.custpage_adjusment;
                    params['custscript_lmry_co_may_bal_anu_digits'] = context.request.parameters.custpage_digits;
                    /* ***********************************************************************************
                     * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_co_may_bal_anu_multi'] = context.request.parameters.custpage_multibook;
                    }
                }

                if (idrpts == 36) {
                    if (arr_id[2] == 10) {

                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_co_f1001_ret_v10_subsi'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_co_f1001_ret_v10_period'] = context.request.parameters.custpage_txtanio;
                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_co_f1001_ret_v10_multi'] = context.request.parameters.custpage_multibook;
                        }
                        params['custscript_lmry_co_f1001_ret_v10_rptid'] = idrpts;

                        params['custscript_lmry_co_f1001_ret_v10_rptvid'] = arr_id[2];
                        params['custscript_lmry_co_f1001_ret_v10_logid'] = rec_id;
                        params['custscript_lmry_co_f1001_ret_v10_concep'] = context.request.parameters.custpage_concept;
                    }
                }


                if (arr_id[2] == 10) {
                    // Reporte Form 1001 Pagos o abonos en cuenta y retenciones practicadas - Anual V10
                    params['custscript_lmry_form1001_v10_periodo'] = context.request.parameters.custpage_txtanio;
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_form1001_v10_subsidiaria'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_form1001_v10_idrpt'] = idrpts;
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_form1001_v10_multibook'] = context.request.parameters.custpage_multibook;
                    }
                    params['custscript_lmry_form1001_v10_concepto'] = context.request.parameters.custpage_concept;
                    params['custscript_lmry_form1001_v10_feat_versio'] = arr_id[2];
                    params['custscript_lmry_form1001_v10_logid'] = rec_id;

                } else if (arr_id[2] == 3) {
                    // Reporte Form 1001 Pagos o abonos en cuenta y retenciones practicadas - Anual v3.0
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_subsidi_form1001anual_v3'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_periodo_form1001anual_v3'] = context.request.parameters.custpage_txtanio;
                    params['custscript_lmry_idrpt_form1001anual_v3'] = rec_id;
                    /* ***********************************************************************************
                     * 2018/05/29 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_multibk_form1001anual_v3'] = context.request.parameters.custpage_multibook;
                    }

                } else if (arr_id[2] == 5) {
                    // Reporte Form 1005 Impuesto a las ventas por pagar (Descontable) - Anual v7.1
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_co_1005_anualv7_subsi'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_co_1005_anualv7_periodo'] = context.request.parameters.custpage_txtanio;
                    params['custscript_lmry_co_1005_anualv7_idlog'] = rec_id;
                    params['custscript_lmry_co_1005_anualv7_idrepor'] = idrpts;
                    params['custscript_lmry_co_1005_anualv7_idfbv'] = arr_id[2];
                    params['custscript_lmry_co_1005_anualv7_concept'] = context.request.parameters.custpage_concept;
                    /* ***********************************************************************************
                     * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_co_1005_multi_anualv7'] = context.request.parameters.custpage_multibook;
                    }

                } else if (arr_id[2] == 14) {
                    // Reporte Form 1005 Impuesto a las ventas por pagar (Descontable) - Anual v3
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_subsidi_form1005anual_v3'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_anio_form1005anual_v3'] = context.request.parameters.custpage_txtanio;
                    params['custscript_lmry_idrpt_form1005anual_v3'] = rec_id;
                    /* ***********************************************************************************
                     * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_multi_form1005anual_v3'] = context.request.parameters.custpage_multibook;
                    }

                } else if (arr_id[2] == 6) {
                    /* ***********************************************************************************
                     * 2019/04/09 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_co_1006_anual_multi'] = context.request.parameters.custpage_multibook;
                    }
                    // Reporte Form 1006 Impuesto a las ventas por pagar (Generado) - Anual v8.1
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_co_1006_anual_subsi'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_co_1006_anual_period'] = context.request.parameters.custpage_txtanio;
                    params['custscript_lmry_co_1006_anual_idlog'] = rec_id;
                    params['custscript_lmry_co_1006_anual_idrpt'] = idrpts;
                    params['custscript_lmry_co_1006_anual_idfbv'] = arr_id[2];
                    params['custscript_lmry_co_1006_anual_concep'] = context.request.parameters.custpage_concept;

                } else if (arr_id[2] == 8) {
                    // Reporte Form 1006 Impuesto a las ventas por pagar - Anual v3
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_form1006anual_subs_v3'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_form1006anual_peri_v3'] = context.request.parameters.custpage_txtanio;
                    params['custscript_lmry_form1006anual_idrp_v3'] = rec_id;
                    /* ***********************************************************************************
                     * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/

                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_form1006anual_multi_v3'] = context.request.parameters.custpage_multibook;
                    }
                } else if (arr_id[2] == 4) {
                    LOG.debug('1007', '1007 correcto');
                    // Reporte Form 1007  INGRESOS RECIBIDOS (Descontable) - Anual v9.0
                    // if (featuresubs == true || featuresubs == 'T') {
                    //     params['custscript_lmry_subsi_form1007anual_v9'] = context.request.parameters.custpage_subsidiary;;
                    // }
                    // params['custscript_lmry_periodo_form1007anual_v9'] = context.request.parameters.custpage_txtanio;
                    // params['custscript_lmry_idlog_form1007anual_v9'] = rec_id;
                    // params['custscript_lmry_feature_form1007anual_v9'] = idrpts;
                    // params['custscript_lmry_idfbv_form1007anual_v9'] = arr_id[2];
                    // params['custscript_lmry_concept_form1007anual_v9'] = context.request.parameters.custpage_concept;

                    // /* ***********************************************************************************
                    //  * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                    //  * **********************************************************************************/
                    // //var Multibook = objContext.getFeature('multibook')
                    // if (featuremult == true || featuremult == 'T') {
                    //     params['custscript_lmry_multi_form1007anual_v9'] = context.request.parameters.custpage_multibook;
                    // }
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_subs_form1007anualv9'] = context.request.parameters.custpage_subsidiary;;
                    }
                    params['custscript_lmry_periodo_form1007anualv9'] = context.request.parameters.custpage_txtanio;
                    params['custscript_lmry_idlog_form1007anualv9'] = rec_id;
                    params['custscript_lmry_feature_form1007anualv9'] = idrpts;
                    params['custscript_lmry_idfbv_form1007anualv9'] = arr_id[2];
                    params['custscript_lmry_concept_form1007anualv9'] = context.request.parameters.custpage_concept;
                    params['custscript_lmry_detalla_form1007anualv9'] = context.request.parameters.custpage_unificado_1007;

                    /* ***********************************************************************************
                     * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/
                    //var Multibook = objContext.getFeature('multibook')
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_multi_form1007anualv9'] = context.request.parameters.custpage_multibook;
                    }

                } else if (arr_id[2] == 9) {
                    // Reporte Form 1007 Saldo de Cuentas por Cobrar - Anual v3.0
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_subsidi_form1007anual_v3'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_anio_form1007anual_v3'] = context.request.parameters.custpage_txtanio;
                    params['custscript_lmry_idrpt_form1007anual_v3'] = rec_id;
                    /* ***********************************************************************************
                     * 2018/05/29 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/

                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_multi_form1007anual_v3'] = context.request.parameters.custpage_multibook;
                    }
                } else if (arr_id[2] == 7) {
                    // Reporte Form 1008 Saldo de Cuentas por Cobrar - Anual v7.1
                    // if (featuresubs == true || featuresubs == 'T') {
                    //     params['custscript_lmry_co_1008_anual_71_subsi'] = context.request.parameters.custpage_subsidiary;
                    // }
                    // params['custscript_lmry_co_1008_anual_71_periodo'] = context.request.parameters.custpage_txtanio;
                    // params['custscript_lmry_co_1008_anual_71_idlog'] = rec_id;
                    // params['custscript_lmry_co_1008_anual_71_idrepor'] = idrpts;
                    // params['custscript_lmry_co_1008_anual_71_idfbv'] = arr_id[2];
                    // params['custscript_lmry_co_1008_anual_71_concept'] = context.request.parameters.custpage_concept;

                    // /* ***********************************************************************************
                    //  * 2019/04/10 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                    //  * **********************************************************************************/

                    // if (featuremult == true || featuremult == 'T') {
                    //     params['custscript_lmry_co_1008_anual_71_multi'] = context.request.parameters.custpage_multibook;
                    // }

                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_subs_co_1008anualv71'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_period_co_1008anualv71'] = context.request.parameters.custpage_txtanio;
                    params['custscript_lmry_idlog_co_1008anualv71'] = rec_id;
                    params['custscript_lmry_feature_co_1008anualv71'] = idrpts;
                    params['custscript_lmry_idfbv_co_1008anualv71'] = arr_id[2];
                    params['custscript_lmry_concept_co_1008anualv71'] = context.request.parameters.custpage_concept;

                    /* ***********************************************************************************
                     * 2019/04/10 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/

                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_multib_co_1008anualv71'] = context.request.parameters.custpage_multibook;
                    }

                } else if (arr_id[2] == 15) {
                    // Reporte Form 1008 Saldo de Cuentas por Cobrar - Anual v3.0
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_subsidi_form1008anual_v3'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_periodo_form1008anual_v3'] = context.request.parameters.custpage_txtanio;
                    params['custscript_lmry_idrpt_form1008anual_v3'] = rec_id;
                    /* ***********************************************************************************
                     * 2018/05/29 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/

                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_multibk_form1008anual_v3'] = context.request.parameters.custpage_multibook;
                    }
                } else if (arr_id[2] == 12) {
                    // Reporte Form 1009 Saldo de Cuentas por Pagar - Anual v7.1
                    /*
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_co_1009_anual_71_subsi'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_co_1009_anual_71_periodo'] = context.request.parameters.custpage_txtanio;
                    params['custscript_lmry_co_1009_anual_71_idlog'] = rec_id;
                    params['custscript_lmry_co_1009_anual_71_idrepor'] = idrpts;
                    params['custscript_lmry_co_1009_anual_71_idfbv'] = arr_id[2];
                    params['custscript_lmry_co_1009_anual_71_concept'] = context.request.parameters.custpage_concept;
                    */

                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_1009_v71_subsi'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_1009_v71_period'] = context.request.parameters.custpage_txtanio;
                    params['custscript_lmry_1009_v71_idlog'] = rec_id;
                    params['custscript_lmry_1009_v71_rpt'] = idrpts;
                    params['custscript_lmry_1009_v71_byversion'] = arr_id[2];
                    params['custscript_lmry_1009_v71_concept'] = context.request.parameters.custpage_concept;

                    /* ***********************************************************************************
                     * 2019/04/10 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/
                    /*
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_co_1009_anual_71_multi'] = context.request.parameters.custpage_multibook;
                    }*/
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_1009_v71_multi'] = context.request.parameters.custpage_multibook;
                    }

                } else if (arr_id[2] == 16) {
                    // Reporte Form 1009 Saldo de Cuentas por Pagar - anual-- V3
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_subsidi_form1009anual_v3'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_anio_form1009anual_v3'] = context.request.parameters.custpage_txtanio;
                    params['custscript_lmry_idrpt_form1009anual_v3'] = rec_id;
                    /* ***********************************************************************************
                     * 2018/03/22 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/

                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_multi_form1009anual_v3'] = context.request.parameters.custpage_multibook;
                    }
                } else if (arr_id[2] == 11) {
                    // Reporte Form 1012  INFORMACIÓN DE LAS DECLARACIONES TRIBUTARIAS, ACCIONES Y APORTES E
                    // INVERSIONES EN BONOS, CERTIFICADOS, TITULOS Y DEMAS INVERSIONES TRIBUTARIAS  - Anual v7.1
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_subsid_form101_mprd'] = context.request.parameters.custpage_subsidiary;;
                    }
                    params['custscript_lmry_period_form1012anu_m_v71'] = context.request.parameters.custpage_txtanio;
                    params['custscript_lmry_idlog_form1012anu_m_v71'] = rec_id;
                    params['custscript_lmry_idrepo_form1012anu_m_v71'] = idrpts;
                    params['custscript_lmry_byvers_form1012anu_m_v71'] = arr_id[2];
                    params['custscript_lmry_concep_form1012anu_m_v71'] = context.request.parameters.custpage_concept;

                    /* ***********************************************************************************
                     * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/
                    //var Multibook = objContext.getFeature('multibook')
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_multi_form1012_mprd'] = context.request.parameters.custpage_multibook;
                    }
                } else if (arr_id[2] == 17) {
                    // Reporte Form 1012 Declaraciones Tributarias - anual v3.0
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_co_subsidi_f1012anual_v3'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_co_anio_f1012anual_v3'] = context.request.parameters.custpage_txtanio;
                    params['custscript_lmry_co_idrpt_f1012anual_v3'] = rec_id;
                    /* ***********************************************************************************
                     * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/

                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_co_multib_f1012anual_v3'] = context.request.parameters.custpage_multibook;
                    }
                } else if (arr_id[2] == 18) {
                    // Reporte Form 1003 Saldo de Cuentas por Cobrar - Anual v3.0
                    /*if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_subsidi_form1003anual_v3'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_anio_form1003anual_v3'] = context.request.parameters.custpage_txtanio;
                    params['custscript_lmry_idrpt_form1003anual_v3'] = rec_id;*/
                    /* ***********************************************************************************
                     * 2018/05/29 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/

                    /*if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_multi_form1003anual_v3'] = context.request.parameters.custpage_multibook;
                    }*/
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_co_rpt_1003_subsidiaria'] = context.request.parameters.custpage_subsidiary;
                    }
                    params['custscript_lmry_co_rpt_1003_periodo'] = context.request.parameters.custpage_txtanio;
                    params['custscript_lmry_co_rpt_1003_recordid'] = rec_id;
                    params['custscript_lmry_co_rpt_1003_feature'] = idrpts;
                    params['custscript_lmry_co_rpt_1003_concepto'] = context.request.parameters.custpage_concept;
                    params['custscript_lmry_co_rpt_1003_by_version'] = arr_id[2];
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_co_rpt_1003_multibook'] = context.request.parameters.custpage_multibook;
                    }
                } else if (arr_id[2] == 19) {
                    if (featuresubs == true || featuresubs == 'T') {
                        params['custscript_lmry_co_1003_anual_10_subsi'] = context.request.parameters.custpage_subsidiary;;
                    }
                    params['custscript_lmry_co_1003_anual_10_periodo'] = context.request.parameters.custpage_txtanio;
                    params['custscript_lmry_co_1003_anual_10_idlog'] = rec_id;
                    params['custscript_lmry_co_1003_anual_10_idrepor'] = idrpts;
                    params['custscript_lmry_co_1003_anual_10_idfbv'] = arr_id[2];
                    params['custscript_lmry_co_1003_anual_10_concept'] = context.request.parameters.custpage_concept;

                    /* ***********************************************************************************
                     * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                     * **********************************************************************************/
                    //var Multibook = objContext.getFeature('multibook')
                    if (featuremult == true || featuremult == 'T') {
                        params['custscript_lmry_co_1003_anual_10_multi'] = context.request.parameters.custpage_multibook;
                    }
                } else {
                    if (idrpts != 41 && idrpts != 42 && idrpts != 26 && idrpts != 22 && idrpts != 23 && idrpts != 24 && idrpts != 25 && idrpts != 51&& idrpts != 57 && idrpts != 58) {
                        ////////////////////////Reportes no usables///////////////////////////////////////////////////////////////////////////////////
                        // Reporte Form 1006 Impuesto a las ventas
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_form1006_sub'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_form1006_per'] = context.request.parameters.custpage_periodo;
                        params['custscript_lmry_form1006_rpt'] = rec_id;

                        // Reporte Form 1012 Declaraciones Tributarias
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_form1012'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_periodo_form1012'] = context.request.parameters.custpage_periodo;
                        params['custscript_lmry_idrpt_form1012'] = rec_id;
                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_mulltibooking_form1012'] = context.request.parameters.custpage_multibook;
                        }
                        // Reporte Form 1012 Declaraciones Tributarias - anual
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_form1012anual'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_anio_form1012anual'] = context.request.parameters.custpage_txtanio;
                        params['custscript_lmry_idrpt_form1012anual'] = rec_id;
                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_multibooki_form1012anual'] = context.request.parameters.custpage_multibook;
                        }

                        // Reporte Form 1012 Declaraciones Tributarias - anual v2.0
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_form1012anual_20'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_anio_form1012anual_20'] = context.request.parameters.custpage_txtanio;
                        params['custscript_lmry_idrpt_form1012anual_20'] = rec_id;
                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_multi_form1012anual_20'] = context.request.parameters.custpage_multibook;
                        }

                        // Reporte Form 1005 Impuesto a las ventas por pagar (Descontable)
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_form1005'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_periodo_form1005'] = context.request.parameters.custpage_periodo;
                        params['custscript_lmry_idrpt_form1005'] = rec_id;

                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_multibooking_form1005'] = context.request.parameters.custpage_multibook;
                        }

                        // Reporte Form 1005 Impuesto a las ventas por pagar (Descontable) - Anual
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_form1005anual'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_anio_form1005anual'] = context.request.parameters.custpage_txtanio;
                        params['custscript_lmry_idrpt_form1005anual'] = rec_id;
                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_multibooki_form1005anual'] = context.request.parameters.custpage_multibook;
                        }

                        // Reporte Form 1005 Impuesto a las ventas por pagar (Descontable) - Anual v2
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_1005anual_20'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_anio_1005anual_20'] = context.request.parameters.custpage_txtanio;
                        params['custscript_lmry_idrpt_1005anual_20'] = rec_id;
                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_multibooki_1005anual_20'] = context.request.parameters.custpage_multibook;
                        }

                        // Reporte Form 1007 Ingresos Recibidos
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_form1007'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_periodo_form1007'] = context.request.parameters.custpage_periodo;
                        params['custscript_lmry_idrpt_form1007'] = rec_id;


                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_multibooki_form1007anual'] = context.request.parameters.custpage_multibook;
                        }
                        ////////////////////////////////////////////////
                        // Reporte Form 1007 Ingresos Recibidos - anual
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_form1007anual'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_anio_form1007anual'] = context.request.parameters.custpage_txtanio;
                        params['custscript_lmry_idrpt_form1007anual'] = rec_id;


                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_booking_form1007anual'] = context.request.parameters.custpage_multibook;
                        }

                        // Reporte Form 1007 Ingresos Recibidos - anual v2.0
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_form1007anual_20'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_anio_form1007anual_20'] = context.request.parameters.custpage_txtanio;
                        params['custscript_lmry_id_per_form1007anual_20'] = context.request.parameters.custpage_periodo;
                        params['custscript_lmry_idrpt_form1007anual_20'] = rec_id;
                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_multi_form1007anual_20'] = context.request.parameters.custpage_multibook;
                        }

                        // Reporte Form 1001 Pagos o abonos en cuenta y retenciones practicadas
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_form1001'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_periodo_form1001'] = context.request.parameters.custpage_periodo;
                        params['custscript_lmry_idrpt_form1001'] = rec_id;

                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_multibooking_form1001'] = context.request.parameters.custpage_multibook;
                        }

                        // Reporte Form 1001 Pagos o abonos en cuenta y retenciones practicadas - Anual
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_form1001anual'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_anio_form1001anual'] = context.request.parameters.custpage_txtanio;
                        params['custscript_lmry_idrpt_form1001anual'] = rec_id;
                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_multibooking_form1001anu'] = context.request.parameters.custpage_multibook;
                        }

                        // Reporte Form 1001 Pagos o abonos en cuenta y retenciones practicadas - Anual v2.0
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_form1001anual_20'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_anio_form1001anual_20'] = context.request.parameters.custpage_txtanio;
                        params['custscript_lmry_idrpt_form1001anual_20'] = rec_id;
                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_multibooking_form1001_20'] = context.request.parameters.custpage_multibook;
                        }

                        // Reporte Form 1006 Impuesto a las ventas por pagar
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_form1006'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_periodo_form1006'] = context.request.parameters.custpage_anio;
                        params['custscript_lmry_idrpt_form1006'] = rec_id;

                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_form1006_multibooking'] = context.request.parameters.custpage_multibook;
                        }


                        // Reporte Form 1006 Impuesto a las ventas por pagar - Anual
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_form1006anual_subs'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_form1006anual_peri'] = context.request.parameters.custpage_txtanio;
                        params['custscript_lmry_form1006anual_idrp'] = rec_id;
                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_form1006anual_multibooki'] = context.request.parameters.custpage_multibook;
                        }

                        // Reporte Form 1006 Impuesto a las ventas por pagar - Anual
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_form1006anual_subs_2'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_form1006anual_peri_2'] = context.request.parameters.custpage_txtanio;
                        params['custscript_lmry_form1006anual_idrp_2'] = rec_id;
                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_form1006anual_multibook2'] = context.request.parameters.custpage_multibook;
                        }

                        //*************************************************************************************************
                        // Reporte Form 1008 Saldo de Cuentas por Cobrar
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_form1008'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_periodo_form1008'] = context.request.parameters.custpage_periodo;
                        params['custscript_lmry_idrpt_form1008'] = rec_id;
                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_multibooking_form1008'] = context.request.parameters.custpage_multibook;
                        }
                        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                        // 2018/03/07 Reporte Form 1008 Saldo de Cuentas por Cobrar - anual V2
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_co_subsi_form1008anual'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_co_date_form1008anual'] = context.request.parameters.custpage_txtanio;
                        params['custscript_lmry_co_idrpt_form1008anual'] = rec_id;
                        /* ***********************************************************************************
                         * 2018/03/07 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_co_multi_form1008anual'] = context.request.parameters.custpage_multibook;
                        }
                        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

                        // Reporte Form 1008 Saldo de Cuentas por Cobrar - anual
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_form1008anual'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_anio_form1008anual'] = context.request.parameters.custpage_txtanio;
                        params['custscript_lmry_idrpt_form1008anual'] = rec_id;
                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_multibooki_form1008anual'] = context.request.parameters.custpage_multibook;
                        }

                        // Reporte Form 1009 Saldo de Cuentas por Pagar
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_form1009'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_periodo_form1009'] = context.request.parameters.custpage_periodo;
                        params['custscript_lmry_idrpt_form1009'] = rec_id;

                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_mukltibooking_form1009'] = context.request.parameters.custpage_multibook;
                        }
                        // Reporte Form 1009 Saldo de Cuentas por Pagar - anual
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_form1009anual'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_anio_form1009anual'] = context.request.parameters.custpage_txtanio;
                        params['custscript_lmry_idrpt_form1009anual'] = rec_id;
                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_multibooki_form1009anual'] = context.request.parameters.custpage_multibook;
                        }
                        //***********************************************************************************************
                        // Reporte Form 1009 Saldo de Cuentas por Pagar - anual--V2( 2018/03/22)
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_co_subsi_form1009anual'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_co_date_form1009anual'] = context.request.parameters.custpage_txtanio;
                        params['custscript_lmry_co_idrpt_form1009anual'] = rec_id;
                        /* ***********************************************************************************
                         * 2018/03/22 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_co_multi_form1009anual'] = context.request.parameters.custpage_multibook;
                        }

                        // Reporte Form 1003 Retenciones en la fuente que le practicaron
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_form1003'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_periodo_form1003'] = context.request.parameters.custpage_periodo;
                        params['custscript_lmry_idrpt_form1003'] = rec_id;

                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_multibooking_form1003'] = context.request.parameters.custpage_multibook;
                        }

                        // Reporte Form 1003 Retenciones en la fuente que le practicaron - Anual
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_form1003anual'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_anio_form1003anual'] = context.request.parameters.custpage_txtanio;
                        params['custscript_lmry_idrpt_form1003anual'] = rec_id;

                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_multibooki_form1003anual'] = context.request.parameters.custpage_multibook;
                        }
                        ///////////////////////////////////////////////////////////////////////////////////////////////////////////
                        // 27/03/2018  Reporte Form 1003 Retenciones en la fuente que le practicaron - Anual
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_co_subsi_form1003anual'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_co_date_form1003anual'] = context.request.parameters.custpage_txtanio;
                        params['custscript_lmry_co_idrpt_form1003anual'] = rec_id;

                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_co_multi_form1003anual'] = context.request.parameters.custpage_multibook;
                        }

                        // Reporte Certificado de Retencion
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_certreten'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_pinicio_certreten'] = context.request.parameters.custpage_lmry_cr_fechaini;
                        params['custscript_lmry_pfinal_certreten'] = context.request.parameters.custpage_lmry_cr_fechafin;
                        params['custscript_lmry_vendor_certreten'] = context.request.parameters.custpage_lmry_cr_vendor;
                        params['custscript_lmry_treten_certreten'] = context.request.parameters.custpage_tipo_retencion;
                        params['custscript_lmry_idrpt_certreten'] = rec_id;

                        /* *************************************************************************************************************************
                         * 2018/03/02 Reporte Certificado de Retencion con Multibook
                         * ************************************************************************************************************************/
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_co_subsi_withbook'] = context.request.parameters.custpage_subsidiary;
                        }

                        // Reporte Libro de Inventario y Balance
                        params['custscript_lmry_periodo_invbalance'] = context.request.parameters.custpage_periodo;
                        params['custscript_lmry_idrpt_invbalance'] = rec_id;
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_invbalance'] = context.request.parameters.custpage_subsidiary;
                        }
                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_booking_invbalan'] = context.request.parameters.custpage_multibook;
                        }

                        /* *************************************************************************************************************************
                         * 2018/10/29 Libro de Inventario y Balance script 2.0
                         * ************************************************************************************************************************/
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_invbal_subsi'] = context.request.parameters.custpage_subsidiary;
                        }

                        params['custscript_lmry_invbal_periodo'] = context.request.parameters.custpage_custom_period;
                        params['custscript_lmry_invbal_logid'] = rec_id;

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_invbal_multibook'] = context.request.parameters.custpage_multibook;
                        }
                        params['custscript_lmry_invbal_adjust'] = context.request.parameters.custpage_adjusment;

                        // Reporte Libro Diario
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_librodiarioco'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_periodo_librodiarioco'] = context.request.parameters.custpage_periodo;
                        params['custscript_lmry_idrpt_librodiarioco'] = rec_id;

                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_multibook_librodiarioco'] = context.request.parameters.custpage_multibook;
                        }

                        // Reporte Libro Diario sin cierre
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_librodiariocie'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_periodo_librodiariocie'] = context.request.parameters.custpage_periodo;
                        params['custscript_lmry_idrpt_librodiariocie'] = rec_id;
                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_booking_librodiariocie'] = context.request.parameters.custpage_multibook;
                        }


                        // Reporte Libro Mayor y Balance
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_subsidi_mayorbalance'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_periodo_mayorbalance'] = context.request.parameters.custpage_periodo;
                        params['custscript_lmry_idrpt_mayorbalance'] = rec_id;
                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/

                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_multibooking_mayorbalanc'] = context.request.parameters.custpage_multibook;
                        }

                        // Reporte Libro Mayor y Balance Anual
                        if (featuresubs == true || featuresubs == 'T') {
                            params['custscript_lmry_co_may_bal_anu_subsi'] = context.request.parameters.custpage_subsidiary;
                        }
                        params['custscript_lmry_co_may_bal_anu_period'] = context.request.parameters.custpage_periodo;
                        params['custscript_lmry_co_may_bal_anu_record'] = rec_id;
                        /* ***********************************************************************************
                         * 2017/01/27 Verifica si esta activo la funcionalidad: Multibook - accountingbook
                         * **********************************************************************************/
                        if (featuremult == true || featuremult == 'T') {
                            params['custscript_lmry_co_may_bal_anu_multi'] = context.request.parameters.custpage_multibook;
                        }

                    }
                }

                //************************************************************************************************************************
                //*************************************************************************************************************************
                LOG.debug(varIdSchedule);
                LOG.debug(varIdDeploy);

                try {
                    var tasktype;

                    if (idrpts == 43 || idrpts == 38 || idrpts == 37 || idrpts == 50 || idrpts == 52 || idrpts == 53 || idrpts == 54 || idrpts == 55 || (idrpts == 36 && arr_id[2] == 10) || (idrpts == 34 && arr_id[2] == 18) || arr_id[2] == 5 || arr_id[2] == 4 || arr_id[2] == 12 || arr_id[2] == 7) {
                        tasktype = TASK.TaskType.MAP_REDUCE;
                    } else {
                        tasktype = TASK.TaskType.SCHEDULED_SCRIPT;
                    }

                    var RedirecSchdl = TASK.create({
                        taskType: tasktype,
                        scriptId: varIdSchedule,
                        deploymentId: varIdDeploy,
                        params: params
                    });

                    RedirecSchdl.submit();

                    REDIRECT.toSuitelet({

                        scriptId: 'customscript_lmry_co_rpt_gen_v2_0',
                        deploymentId: 'customdeploy_lmry_co_rpt_gen_v2_0'
                    });
                } catch (err) {
                    var varMsgError = 'No se puede procesar dado que hay un proceso pendiente en la cola';
                    LOG.error({
                        title: 'Se genero un error en suitelet:',
                        details: err
                    });

                    //  LIBRARY.CreacionFormError(namereport, LMRY_script, varMsgError, err);

                }

            }
        }

    } catch (err) {
        var varMsgError = 'Importante: El acceso no esta permitido.';
        LOG.error({
            title: 'Se genero un error en suitelet:',
            details: err
        });
        //  LIBRARY.CreacionFormError(namereport, LMRY_script, varMsgError, err);
        //sendemail(err, LMRY_script);
    }
    return true;
}

/* ------------------------------------------------------------------------------------------------------
 * Nota: Valida si existe el folder donde se guardaran los archivos
 * --------------------------------------------------------------------------------------------------- */

function getGlobalLabels() {
    var labels = {
        "periodo": {
            "es": 'Periodo Contable',
            "pt": 'Período Contábil',
            "en": 'Accounting Period'
        },
        "periodoFin": {
            "es": 'Periodo Contable Final',
            "pt": 'Período Contábil Final',
            "en": 'Final Accounting Period'
        },
        "ajuste": {
            "es": 'Ajuste',
            "pt": 'Ajustamento',
            "en": 'Adjustment'
        },
        "tipoEntidad": {
            "es": 'Tipo de Entidad',
            "pt": 'Tipo de Entidade',
            "en": 'Entity Type'
        },
        "criteriosBusqueda": {
            "es": "Criterios de Busqueda",
            "pt": "Critérios de busca",
            "en": "Search Criteria"
        },
        "tiposReporte": {
            "es": "Tipos de Reporte",
            "pt": "Tipos de relatórios",
            "en": "Types of Reports"
        },
        "reporte": {
            "es": "Reporte",
            "pt": "Relatório",
            "en": "Report"
        },
        "subsidiaria": {
            "es": "SUBSIDIARIA",
            "pt": "SUBSIDIÁRIA",
            "en": "SUBSIDIARY"
        },
        "logGeneracion": {
            "es": "Log de generacion",
            "pt": "Log de geração",
            "en": "Generation log"
        },
        "fechaCreacion": {
            "es": "Fecha de creacion",
            "pt": "Data de criação",
            "en": "Date of creation"
        },
        "criteriosEspeciales": {
            "es": "Criterios Especiales",
            "pt": "Critérios especiais",
            "en": "Special Criteria"
        },
        "informe": {
            "es": "Informe",
            "pt": "Informe",
            "en": "Report"
        },
        "periodoLog": {
            "es": "Periodo",
            "pt": "Período",
            "en": "Period"
        },
        "subsidiariaLog": {
            "es": "Subsidiaria",
            "pt": "Subsidiária",
            "en": "Subsidiary"
        },
        "creadoPor": {
            "es": "Creado por",
            "pt": "Criado por",
            "en": "Created by"
        },
        "nombreArchivo": {
            "es": "Nombre archivo",
            "pt": "Nome do ficheiro",
            "en": "File name"
        },
        "descargar": {
            "es": "Descargar",
            "pt": "Descarregar",
            "en": "Download"
        },
        "btnGenerar": {
            "es": "Generar",
            "pt": "Gerar",
            "en": "Generate"
        },
        "btnCancelar": {
            "es": "Cancelar",
            "pt": "Cancelar",
            "en": "Cancel"
        },
        "btnCancelar": {
            "es": "Cancelar",
            "pt": "Cancelar",
            "en": "Cancel"
        },
        "custpage_lmry_cr_vendor": {
            "es": "Vendor",
            "pt": "Fornecedor",
            "en": "Vendor"
        },
        "custpage_tipo_retencion": {
            "es": "Tipo Retencion",
            "pt": "Tipo de Retenção",
            "en": "Type of retention"
        },
        "custpage_lmry_cr_fechaini": {
            "es": "Fecha desde",
            "pt": "Data desde",
            "en": "Date from"
        },
        "custpage_lmry_cr_fechafin": {
            "es": "Fecha Hasta",
            "pt": "Data até",
            "en": "Date Until"
        },
        "custpage_digits": {
            "es": 'HABILITAR PUC 8 DIGITOS',
            "pt": 'ATIVAR PUC 8 DÍGITOS',
            "en": 'ENABLE PUC 8 DIGITS'
        },
        "filterHelp": {
            "es": 'Generar reporte con PUC de 8 dígitos',
            "pt": 'Gerar relatório com PUC de 8 dígitos',
            "en": 'Generate report with 8-digit PUC'
        },
        "custpage_op_balance": {
            "es": 'Saldos Iniciales en Cuenta de Resultados',
            "pt": 'Saldos Iniciales en Cuenta de Resultados',
            "en": 'Initial Balances in Income Statement'
        },
        "agrupadopormes": {
            "es": 'Agrupado por mes',
            "pt": 'Agrupacao por meses',
            "en": 'Grouping by months'
        }
    }

    return labels;
}

function busquedaVersion(idrpts, context) {


    var DbolStop = false;
    var arrAllVersions = new Array();
    var cont = 0;

    var savedSearch = SEARCH.create({
        type: 'customrecord_lmry_co_rpt_feature_version',
        columns: [
            SEARCH.createColumn({
                name: 'custrecord_lmry_co_rpt_id_schedule'
            }),
            SEARCH.createColumn({
                name: 'custrecord_lmry_co_rpt_id_deploy'
            }),
            SEARCH.createColumn({
                name: 'custrecord_lmry_co_rpt_id_report'
            }),
            SEARCH.createColumn({
                name: 'custrecord_lmry_co_rpt_version'
            }),
            SEARCH.createColumn({
                name: 'custrecord_lmry_co_year_from'
            }),
            SEARCH.createColumn({
                name: 'custrecord_lmry_co_year_to'
            }),
            SEARCH.createColumn({
                name: 'internalid'
            })
        ]
    });

    var searchResult = savedSearch.run();

    while (!DbolStop) {

        var objResult = searchResult.getRange(0, 1000);

        if (objResult != null) {

            var intLength = objResult.length;

            if (intLength == 0) {
                DbolStop = true;
            }

            for (var i = 0; i < intLength; i++) {
                var columnas = objResult[i].columns;
                var arrAuxiliar = new Array();

                //0. id SCHDL
                if (objResult[i].getValue(columnas[0]) != null) {
                    arrAuxiliar[0] = objResult[i].getValue(columnas[0]);
                } else {
                    arrAuxiliar[0] = '';
                } //1. id DEPLOY
                if (objResult[i].getValue(columnas[1]) != null) {
                    arrAuxiliar[1] = objResult[i].getValue(columnas[1]);
                } else {
                    arrAuxiliar[1] = '';
                } //2. id RPT
                if (objResult[i].getValue(columnas[2]) != null) {
                    arrAuxiliar[2] = objResult[i].getValue(columnas[2]);
                } else {
                    arrAuxiliar[2] = '';
                } //3. Version del Reporte
                if (objResult[i].getValue(columnas[3]) != null) {
                    arrAuxiliar[3] = objResult[i].getValue(columnas[3]);
                } else {
                    arrAuxiliar[3] = '';
                } //4. PERIODO DESDE
                if (objResult[i].getValue(columnas[4]) != null) {
                    arrAuxiliar[4] = objResult[i].getValue(columnas[4]);
                } else {
                    arrAuxiliar[4] = '';
                } //5. PERIODO HASTA
                if (objResult[i].getValue(columnas[5]) != null) {
                    arrAuxiliar[5] = objResult[i].getValue(columnas[5]);
                } else {
                    arrAuxiliar[5] = '';
                }
                if (objResult[i].getValue(columnas[6]) != null) {
                    arrAuxiliar[6] = objResult[i].getValue(columnas[6]);
                } else {
                    arrAuxiliar[6] = '';
                }

                arrAllVersions[cont] = arrAuxiliar;
                cont++;
            }

            if (intLength < 1000) {
                DbolStop = true;
            }


        }
    }

    for (var i = 0; i < arrAllVersions.length; i++) {
        if (arrAllVersions[i][2] == idrpts) {

            var anio = context.request.parameters.custpage_txtanio;
            var Date1 = FORMAT.parse({
                value: arrAllVersions[i][4],
                type: FORMAT.Type.DATE
            });
            var Date2 = FORMAT.parse({
                value: arrAllVersions[i][5],
                type: FORMAT.Type.DATE
            });
            var year_from = Date1.getFullYear();
            var year_to = Date2.getFullYear();

            if ((Number(anio) >= Number(year_from) || (Number(year_to) == '' || Number(year_to) == null)) && ((Number(year_from) == '' || Number(year_from) == null) || Number(anio) <= Number(year_to))) {
                var nuevoidSCHL = arrAllVersions[i][0];
                var nuevoidDEPLOY = arrAllVersions[i][1];
                var nuevointernalId = arrAllVersions[i][6];
            }

        }

    }

    return [nuevoidSCHL, nuevoidDEPLOY, nuevointernalId];


}

function search_folder() {
    try {
        // Ruta de la carpeta contenedora

        var varScriptObj = RUNTIME.getCurrentScript();
        var FolderId = varScriptObj.getParameter({
            name: 'custscript_lmry_file_cabinet_rg_co'
        });



        if (FolderId == '' || FolderId == null) {

            // Valida si existe "SuiteLatamReady" en File Cabinet
            var varIdFolderPrimary = '';

            var ResultSet = SEARCH.create({
                type: 'folder',
                columns: ['internalid'],
                filters: ['name', 'is', 'SuiteLatamReady']
            });

            objResult = ResultSet.run().getRange(0, 50);

            if (objResult == '' || objResult == null) {
                var varRecordFolder = RECORD.create({
                    type: 'folder'
                });
                varRecordFolder.setValue('name', 'SuiteLatamReady');
                varIdFolderPrimary = varRecordFolder.save();
            } else {
                varIdFolderPrimary = objResult[0].getValue('internalid');
            }

            // Valida si existe "LMRY Report Generator" en File Cabinet
            var varFolderId = '';
            var ResultSet = SEARCH.create({
                type: 'folder',
                columns: ['internalid'],
                filters: [
                    ['name', 'is', 'Latam Report Generator CO v2.0']
                ]
            });
            objResult = ResultSet.run().getRange(0, 50);

            if (objResult == '' || objResult == null) {
                var varRecordFolder = RECORD.create({
                    type: 'folder'
                });
                varRecordFolder.setValue('name', 'Latam Report Generator CO v2.0');
                varRecordFolder.setValue('parent', varIdFolderPrimary);
                varFolderId = varRecordFolder.save();
            } else {
                varFolderId = objResult[0].getValue('internalid');
            }


            // Load the NetSuite Company Preferences page
            var varCompanyReference = CONFIG.load({
                type: CONFIG.Type.COMPANY_PREFERENCES
            });

            // set field values
            varCompanyReference.setValue({
                fieldId: 'custscript_lmry_file_cabinet_rg_co',
                value: varFolderId
            });
            // save changes to the Company Preferences page
            varCompanyReference.save();
        }
    } catch (err) {

        LOG.error({
            title: 'Se genero un error en suitelet',
            details: err
        });
        // Mail de configuracion del folder
        //LIBRARY.sendMail(LMRY_script, ' [ onRequest ] ' + err);
        LIBFEATURE.sendErrorEmail(err, LMRY_script, language);
    }
    return true;
}

function TraePeriodo(periodo) {

    if (periodo.length == 1) {
        periodo = '0' + periodo;
    }

    var mes = '';
    switch (periodo) {
        case '01':
            mes = 'Jan';
            break;
        case '02':
            mes = 'Feb';
            break;
        case '03':
            mes = 'Mar';
            break;
        case '04':
            mes = 'Apr';
            break;
        case '05':
            mes = 'May';
            break;
        case '06':
            mes = 'Jun';
            break;
        case '07':
            mes = 'Jul';
            break;
        case '08':
            mes = 'Aug';
            break;
        case '09':
            mes = 'Sep';
            break;
        case '10':
            mes = 'Oct';
            break;
        case '11':
            mes = 'Nov';
            break;
        case '12':
            mes = 'Dec';
            break;

    }
    //nlapiLogExecution('DEBUG', 'auxmess2-> ',auxmess);
    return mes;
}