/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_ART2_MPRD_v2.0.js                        ||
||                                                              ||
||  Version Date         Author        Remarks                  ||
||  2.0     Sep 06 2020  LatamReady    Use Script 2.0           ||
\= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.0
 * @NScriptType MapReduceScript
 * @NModuleScope Public
 */
define(['N/search', 'N/log', "N/config", 'require', 'N/file', 'N/runtime', 'N/query', "N/format", "N/record", "N/task", "./CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js",
    "/SuiteBundles/Bundle 37714/Latam_Library/LMRY_LibraryReport_LBRY_V2.js"],

    function (search, log, config, require, fileModulo, runtime, query, format, recordModulo, task, libreria, libreriaFeature) {

        var objContext = runtime.getCurrentScript();
        var LMRY_script = "LMRY_CO_ART2_MPRD_v2.0.js";

        // Parámetros
        var param_RecorID = null;
        var param_Anual = null;
        var param_Multi = null;
        var param_FeatID = null;
        var param_Subsi = null;
        var param_head = null;

        // Features
        var feature_Subsi = null;
        var feature_Multi = null;
        var featureSpecialPeriod = null;
        var isMultiCalendar = null;

        // Fórmula de Periodos
        var formulPeriodFilters = null;

        // Period Data
        var periodenddate = null;
        var periodname = null;
        var periodstartdate = null;
        var Anual = '';

        // Datos de Subsidiaria
        var companyruc = '';
        var companyname = '';

        //Language
        var language = runtime.getCurrentScript().getParameter({
            name: 'LANGUAGE'
        }).substring(0, 2);

        if (language != "en" && language != "es" && language != "pt") {
            language = "es";
        }

        function getInputData() {
            try {
                getParameterAndFeatures();
                var arrTransactions = getTransactions();

                if (arrTransactions.length != 0) {
                    return arrTransactions;
                } else {
                    NoData();
                    return null;
                }

            } catch (err) {
                log.error('err', err);
                libreria.sendMail(LMRY_script, ' [ getInputData ] ' + err);
            }
        }


        function map(context) {
            try {
                getParameterAndFeatures();
                var key = context.key;
                strCreditos = '';
                var arrTransaction = new Array();
                var Vendor = new Array();
                var arrTemp = JSON.parse(context.value);
                datos = DatosCustomer(arrTemp[0]);
                datos_d = datos.split('|');


                if (arrTemp[13] == 'journalentry') {
                    var Journal = search.lookupFields({
                        type: search.Type.VENDOR,
                        id: arrTemp[0],
                        columns: ["custentity_lmry_sunat_tipo_doc_id.custrecord_lmry_co_idtype_name", 'isperson']
                    });

                    var ISperson = Journal.isperson;
                    //0. VIGENCIA
                    var campo0 = (arrTemp[10]);
                    if (ISperson) {
                        var camposJournal1 = search.lookupFields({
                            type: search.Type.VENDOR,
                            id: arrTemp[0],
                            columns: ["custentity_lmry_sunat_tipo_doc_id.custrecord_lmry_co_idtype_name", 'firstname', 'lastname', 'vatregnumber', 'phone', 'email']
                        });

                        //1.tipo de documento
                        var ide = camposJournal1["custentity_lmry_sunat_tipo_doc_id.custrecord_lmry_co_idtype_name"];
                        if (ide != '' && ide != null && ide != 'NaN') {
                            ide = camposJournal1["custentity_lmry_sunat_tipo_doc_id.custrecord_lmry_co_idtype_name"];
                        } else {
                            ide = '';
                        }
                        if (ide == 'CC' || ide == 'CE' || ide == 'TI' || ide == 'NIT' || ide == 'PA') {
                            ide = completar_espacio(3, ide);
                        } else {
                            ide = '';
                        }
                        var campo1 = ide;

                        //2. NÚMERO DOCUMENTO
                        var campo2 = camposJournal1.vatregnumber;
                        campo2 = QuitaGuion(campo2).substring(0, 11);

                        //3. NOMBRE O RAZÓN SOCIAL
                        var first = camposJournal1.firstname;
                        if (first != '' && first != null && first != 'NaN') {
                            first = camposJournal1.firstname;
                        } else {
                            first = '';
                        }

                        var last = camposJournal1.lastname;
                        if (last != '' && last != null && last != 'NaN') {
                            last = camposJournal1.lastname;
                        } else {
                            last = '';
                        }

                        var campo3 = first + ' ' + last;
                        campo3 = Remplaza_tildes(campo3);
                        campo3 = Valida_caracteres_blanco(campo3);
                        campo3 = campo3.substring(0, 70);

                        //4. DIRECCIÓN DE NOTIFICACIÓN
                        var campo4 = (datos_d[0]);

                        //5. TELÉFONO
                        var campo5 = camposJournal1.phone;
                        campo5 = QuitaGuion(campo5);
                        campo5 = campo5.substring(0, 10);

                        //6. E-MAIL
                        var campo6 = camposJournal1.email;

                        //7. CÓDIGO MUNICIPIO
                        var campo7 = datos_d[1];

                        //8. CÓDIGO DEPTO
                        var campo8 = datos_d[2];

                        //9.  CONCEPTO PAGO O ABONO EN CUENTA
                        if (arrTemp[7] != '' && arrTemp[7] != null && arrTemp[7] != 'NaN') {
                            var concepto = search.lookupFields({
                                type: 'customrecord_lmry_payment_type_pa',
                                id: arrTemp[7],
                                columns: ["custrecord1394"]
                            });

                            var id_concepto = concepto.custrecord1394;

                        } else {
                            var id_concepto = '';
                        }

                        var campo9 = id_concepto;

                        //10. VALOR COMPRAS ANUAL
                        var campo10 = Number(arrTemp[8]);

                        //11. VALOR DEVOLUCIONES
                        var campo11 = Number(arrTemp[9]);


                        //12. campo ID vendor y del name del journal
                        var campo12 = arrTemp[0];

                        var CAMPOÑAO = search.lookupFields({
                            type: search.Type.VENDOR,
                            id: arrTemp[0],
                            columns: ['custentity_lmry_country']
                        });

                        var pais = CAMPOÑAO.custentity_lmry_country[0].value;


                        if (pais == '21') {
                            var zonaFranca = true;
                        } else {
                            if (arrTemp[12] != '' && arrTemp[12] != null && arrTemp[12] != 'NaN') {
                                var campozona = search.lookupFields({
                                    type: 'customrecord_lmry_co_transaction_fields',
                                    id: arrTemp[12],
                                    columns: ['custrecord_lmry_co_zonafranca']
                                });

                                var zonaFranca = campozona.custrecord_lmry_co_zonafranca;
                            } else {
                                var zonaFranca = false;
                            }

                        }

                        if (zonaFranca == 'T' || zonaFranca == true) {
                            strCreditos = campo0 + '|' + campo1 + '|' + campo2 + '|' + campo3 + '|' + campo4 + '|' + campo5 + '|' + campo6 + '|' + campo7 + '|' + campo8 + '|' + campo9 + '|' + campo10 + '|' + campo11 + '|' + campo12 + ';'; //+ '\r\n';
                        }


                    } else {
                        var camposJournal2 = search.lookupFields({
                            type: search.Type.VENDOR,
                            id: arrTemp[0],
                            columns: ["custentity_lmry_sunat_tipo_doc_id.custrecord_lmry_co_idtype_name", 'companyname', 'vatregnumber', 'phone', 'email']
                        });

                        //1.tipo de documento
                        var ide = camposJournal2["custentity_lmry_sunat_tipo_doc_id.custrecord_lmry_co_idtype_name"];
                        if (ide != '' && ide != null && ide != 'NaN') {
                            ide = camposJournal2["custentity_lmry_sunat_tipo_doc_id.custrecord_lmry_co_idtype_name"];
                        } else {
                            ide = '';
                        }
                        if (ide == 'CC' || ide == 'CE' || ide == 'TI' || ide == 'NIT' || ide == 'PA') {
                            ide = completar_espacio(3, ide);
                        } else {
                            ide = '';
                        }
                        var campo1 = ide;

                        //2. NÚMERO DOCUMENTO
                        var campo2 = camposJournal2.vatregnumber;
                        campo2 = QuitaGuion(campo2).substring(0, 11);

                        //3. NOMBRE O RAZÓN SOCIAL
                        var raz = camposJournal2.companyname;
                        if (raz != '' && raz != null && raz != 'NaN') {
                            raz = camposJournal2.companyname;
                        } else {
                            raz = '';
                        }

                        var campo3 = raz;
                        campo3 = Remplaza_tildes(campo3);
                        campo3 = Valida_caracteres_blanco(campo3);
                        campo3 = campo3.substring(0, 70);

                        //4. DIRECCIÓN DE NOTIFICACIÓN
                        var campo4 = (datos_d[0]);

                        //5. TELÉFONO
                        var campo5 = camposJournal2.phone;
                        campo5 = QuitaGuion(campo5);
                        campo5 = campo5.substring(0, 10);

                        //6. E-MAIL
                        var campo6 = camposJournal2.email;

                        //7. CÓDIGO MUNICIPIO
                        var campo7 = datos_d[1];

                        //8. CÓDIGO DEPTO
                        var campo8 = datos_d[2];

                        //9.  CONCEPTO PAGO O ABONO EN CUENTA
                        if (arrTemp[7] != '' && arrTemp[7] != null && arrTemp[7] != 'NaN') {
                            var concepto = search.lookupFields({
                                type: 'customrecord_lmry_payment_type_pa',
                                id: arrTemp[7],
                                columns: ["custrecord1394"]
                            });

                            var id_concepto = concepto.custrecord1394;

                        } else {
                            var id_concepto = '';
                        }
                        var campo9 = id_concepto;

                        //10. VALOR COMPRAS ANUAL
                        var campo10 = Number(arrTemp[8]);

                        //11. VALOR DEVOLUCIONES
                        var campo11 = Number(arrTemp[9]);

                        //12. campo ID vendor y del name del journal
                        var campo12 = arrTemp[0];
                        var CAMPOÑAO = search.lookupFields({
                            type: search.Type.VENDOR,
                            id: arrTemp[0],
                            columns: ['custentity_lmry_country']
                        });

                        var pais = CAMPOÑAO.custentity_lmry_country[0].value;

                        if (pais == '21') {
                            var zonaFranca = true;
                        } else {
                            if (arrTemp[12] != '' && arrTemp[12] != null && arrTemp[12] != 'NaN') {
                                var campozona = search.lookupFields({
                                    type: 'customrecord_lmry_co_transaction_fields',
                                    id: arrTemp[12],
                                    columns: ['custrecord_lmry_co_zonafranca']
                                });

                                var zonaFranca = campozona.custrecord_lmry_co_zonafranca;
                            } else {
                                var zonaFranca = false;
                            }
                        }

                        if (zonaFranca == 'T' || zonaFranca == true) {
                            strCreditos = campo0 + '|' + campo1 + '|' + campo2 + '|' + campo3 + '|' + campo4 + '|' + campo5 + '|' + campo6 + '|' + campo7 + '|' + campo8 + '|' + campo9 + '|' + campo10 + '|' + campo11 + '|' + campo12 + ';'; //+ '\r\n';
                        }

                    }

                } else {
                    //0. VIGENCIA
                    var campo0 = (arrTemp[10]);

                    //1. TIPO DOCUMENTO
                    if (arrTemp[1] != '' && arrTemp[1] != null && arrTemp[1] != 'NaN') {
                        var docu = search.lookupFields({
                            type: 'customrecord_lmry_tipo_doc_iden',
                            id: arrTemp[1],
                            columns: ["custrecord_lmry_co_idtype_name"]
                        });

                        var ide = docu.custrecord_lmry_co_idtype_name;

                    } else {
                        var ide = '';
                    }

                    if (ide != '' && ide != null && ide != 'NaN') {
                        ide = ide;
                    } else {
                        ide = '';
                    }
                    log.debug('ide', ide);
                    var campo1 = ide;
                    if (campo1 == 'CC' || campo1 == 'CE' || campo1 == 'TI' || campo1 == 'NIT' || campo1 == 'PA') {
                        campo1 = completar_espacio(3, campo1);
                    } else {
                        campo1 = '';
                    }
                    //2. NÚMERO DOCUMENTO
                    var campo2 = QuitaGuion(arrTemp[2]).substring(0, 11);

                    //3. NOMBRE O RAZÓN SOCIAL
                    var campo3 = Remplaza_tildes(arrTemp[3]);
                    campo3 = Valida_caracteres_blanco(campo3);
                    campo3 = campo3.substring(0, 70);

                    //4. DIRECCIÓN DE NOTIFICACIÓN
                    var campo4 = arrTemp[4];
                    campo4 = campo4.substring(0, 70);
                    campo4 = Valida_caracteres_blanco(campo4)
                    campo4 = Remplaza_tildes(campo4);

                    //5. TELÉFONO
                    var campo5 = QuitaGuion(arrTemp[5]);
                    campo5 = campo5.substring(0, 10);

                    //6. E-MAIL
                    var campo6 = arrTemp[6];

                    //7. CÓDIGO MUNICIPIO
                    var campo7 = datos_d[1];

                    //8. CÓDIGO DEPTO
                    var campo8 = datos_d[2];

                    //9.  CONCEPTO PAGO O ABONO EN CUENTA
                    if (arrTemp[7] != '' && arrTemp[7] != null && arrTemp[7] != 'NaN') {
                        var concepto = search.lookupFields({
                            type: 'customrecord_lmry_payment_type_pa',
                            id: arrTemp[7],
                            columns: ["custrecord1394"]
                        });

                        var id_concepto = concepto.custrecord1394;

                    } else {
                        var id_concepto = '';
                    }
                    var campo9 = id_concepto;

                    //10. VALOR COMPRAS ANUAL
                    var campo10 = Number(arrTemp[8]);

                    //11. VALOR DEVOLUCIONES
                    var campo11 = Number(arrTemp[9]);

                    //12. campo ID vendor y del name del journal
                    var campo12 = arrTemp[0];
                    var CAMPOÑAO = search.lookupFields({
                        type: search.Type.VENDOR,
                        id: arrTemp[0],
                        columns: ['custentity_lmry_country']
                    });

                    var pais = CAMPOÑAO.custentity_lmry_country[0].value;

                    if (pais == '21') {
                        var zonaFranca = true;
                    } else {
                        if (arrTemp[12] != '' && arrTemp[12] != null && arrTemp[12] != 'NaN') {
                            var campozona = search.lookupFields({
                                type: 'customrecord_lmry_co_transaction_fields',
                                id: arrTemp[12],
                                columns: ['custrecord_lmry_co_zonafranca']
                            });

                            var zonaFranca = campozona.custrecord_lmry_co_zonafranca;
                        } else {
                            var zonaFranca = false;
                        }

                    }

                    if (zonaFranca == 'T' || zonaFranca == true) {
                        strCreditos = campo0 + '|' + campo1 + '|' + campo2 + '|' + campo3 + '|' + campo4 + '|' + campo5 + '|' + campo6 + '|' + campo7 + '|' + campo8 + '|' + campo9 + '|' + campo10 + '|' + campo11 + '|' + campo12 + ';'; //+ '\r\n';
                    }

                }

                context.write({
                    key: key,
                    value: {
                        strCreditos: strCreditos

                    }
                });
            } catch (err) {
                log.error('err', err);
                libreria.sendMail(LMRY_script, ' [ Map ] ' + err);
            }
        }


        function reduce(context) {

        }


        function summarize(context) {
            try {
                getParameterAndFeatures();

                var YaHuboArchivosGenerados = false;
                var text = '';
                var FilasRecorridas = 0;
                var rrr = true;
                var cont = 1;
                var text2 = '';

                context.output.iterator().each(function (key, value) {

                    var obj = JSON.parse(value);

                    text += (obj.strCreditos);

                    FilasRecorridas++;

                    var peso = lengthInUtf8Bytes(text);

                    if (FilasRecorridas == 10000) {
                        text2 = agruparFacturas(text);
                        SaveFile(text2, rrr, cont);
                        FilasRecorridas = 0;
                        YaHuboArchivosGenerados = true;
                        text = '';
                        rrr = false;
                        cont = cont + 1;
                    }
                    return true;
                });

                if (FilasRecorridas != 0) {
                    text2 = agruparFacturas(text);
                    if (text2 != '') {
                        SaveFile(text2, rrr, cont);
                    } else {
                        NoData();
                    }
                } else if (!YaHuboArchivosGenerados) {
                    NoData();
                }
            } catch (err) {
                log.error('err', err);
                libreria.sendMail(LMRY_script, ' [ Summarize ] ' + err);
            }
        }

        function agruparFacturas(text) {
            var a = 0;
            var arrCampos = '';
            var auxiliar = text.split(';');
            var arrayAux = new Array();
            var text2 = '';

            for (var i = 0; i < auxiliar.length - 1; i++) {
                var arrCampos = auxiliar[i].split('|');

                var aux = new Array();
                aux[0] = arrCampos[0];
                aux[1] = arrCampos[1];
                aux[2] = arrCampos[2];
                aux[3] = arrCampos[3];
                aux[4] = arrCampos[4];
                aux[5] = arrCampos[5];
                aux[6] = arrCampos[6];
                aux[7] = arrCampos[7];
                aux[8] = arrCampos[8];
                aux[9] = arrCampos[9];
                aux[10] = arrCampos[10];
                aux[11] = arrCampos[11];
                aux[12] = arrCampos[12];


                arrayAux[a] = aux;
                a++;

            }

            for (var a = 0; a < arrayAux.length - 1; a++) {

                for (var j = 0; j < arrayAux.length - 1; j++) {

                    if (Number(arrayAux[j][12] + arrayAux[j][9]) > Number(arrayAux[j + 1][12] + arrayAux[j + 1][9])) {

                        var temp = arrayAux[j + 1];
                        arrayAux[j + 1] = arrayAux[j];
                        arrayAux[j] = temp;
                    }
                }
            }
            log.debug('arrayAux ordenado', arrayAux);

            //**************SUBSIDIARIA********************
            if (feature_Subsi) {
                var subsi_temp = search.lookupFields({
                    type: search.Type.SUBSIDIARY,
                    id: param_Subsi,
                    columns: ['custrecord_lmry_co_uvt_iva']
                });

                var uvt_IVA = subsi_temp.custrecord_lmry_co_uvt_iva;
            } else {

                var configpage = config.load({
                    type: config.Type.COMPANY_INFORMATION
                });

                var uvt_IVA = configpage.getFieldValue('custrecord_lmry_co_uvt_iva');

            }


            log.debug('uvt_IVA', uvt_IVA);
            //**************Agrupa y Suma********************
            for (var j = 0; j < arrayAux.length; j++) {
                var col0 = arrayAux[j][0];
                var col1 = arrayAux[j][1];
                var col2 = arrayAux[j][2];
                var col3 = arrayAux[j][3];
                var col4 = arrayAux[j][4];
                var col5 = arrayAux[j][5];
                var col6 = arrayAux[j][6];
                var col7 = arrayAux[j][7];
                var col8 = arrayAux[j][8];
                var col9 = arrayAux[j][9];
                var col10 = arrayAux[j][10];
                var col11 = arrayAux[j][11];
                var col12 = arrayAux[j][12];

                if (j != arrayAux.length - 1) {

                    while ((Number(arrayAux[j][12] + arrayAux[j][9]) == Number(arrayAux[j + 1][12] + arrayAux[j + 1][9]))) {
                        col10 = Number(col10) + Number(arrayAux[j + 1][10]);
                        col11 = Number(col11) + Number(arrayAux[j + 1][11]);
                        j++;

                        if (j == arrayAux.length - 1) {
                            break;
                        }
                    }
                }

                if (Number(uvt_IVA - 1) < Number(col10)) {

                    if (param_head == 'T' || param_head == true) {
                        text2 += col0 + ',' + col1 + ',' + col2 + ',' + col3 + ',' + col4 + ',' + col5 + ',' + col6 + ',' + col7 + ',' + col8 + ',' + col9 + ',' + Number(col10).toFixed(0) + ',' + Number(col11).toFixed(0) + '\r\n';
                    } else {
                        text2 += col0 + ';' + col1 + ';' + col2 + ';' + col3 + ';' + col4 + ';' + col5 + ';' + col6 + ';' + col7 + ';' + col8 + ';' + col9 + ';' + Number(col10).toFixed(0) + ';' + Number(col11).toFixed(0) + '\r\n';
                    }
                }

            }
            log.debug('text2', text2);
            return text2;

        }

        function NoData() {

            var usuario = runtime.getCurrentUser();

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

            //Periodo
            generatorLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_postingperiod',
                value: periodname
            });

            //Nombre de Archivo
            generatorLog.setValue({
                fieldId: 'custrecord_lmry_co_rg_name',
                value: 'No existe informacion para los criterios seleccionados.'
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

        function lengthInUtf8Bytes(str) {
            var m = encodeURIComponent(str).match(/%[89ABab]/g);
            return str.length + (m ? m.length : 0);
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

        function ObtenerDatosSubsidiaria() {
            var configpage = config.load({
                type: config.Type.COMPANY_INFORMATION
            });

            if (feature_Subsi) {
                companyname = ObtainNameSubsidiaria(param_Subsi);
                companyruc = ObtainFederalIdSubsidiaria(param_Subsi);
            } else {
                companyruc = configpage.getFieldValue('employerid');
                companyname = configpage.getFieldValue('legalname');
            }

            companyruc = companyruc.replace(' ', '');
        }

        function SaveFile(strAuxiliar, rrr, cont) {

            var objContext = runtime.getCurrentScript();
            ObtenerDatosSubsidiaria();

            var folderId = objContext.getParameter({
                name: 'custscript_lmry_file_cabinet_rg_co'
            });

            var report = search.lookupFields({
                type: 'customrecord_lmry_co_features',
                id: param_FeatID,
                columns: ['name']
            });
            namereport = report.name;

            var AAAA = Anual;

            if (feature_Multi) {
                var multibookName_temp = search.lookupFields({
                    type: search.Type.ACCOUNTING_BOOK,
                    id: param_Multi,
                    columns: ['name']
                });

                var multibookName = multibookName_temp.name;
            }

            if (param_head == 'T' || param_head == true) {

                if (feature_Multi) {
                    if (cont == 1) {
                        var fileName = 'ART2' + '_' + companyruc + '_' + AAAA + '_' + param_Subsi + '_' + param_Multi + '.csv';
                    } else {
                        var fileName = 'ART2' + '_' + companyruc + '_' + AAAA + '_' + param_Subsi + '_' + param_Multi + '_' + cont + '.csv';
                    }
                } else {
                    if (cont == 1) {
                        var fileName = 'ART2' + '_' + companyruc + '_' + AAAA + '_' + param_Subsi + '.csv';
                    } else {
                        var fileName = 'ART2' + '_' + companyruc + '_' + AAAA + '_' + param_Subsi + '_' + cont + '.csv';
                    }
                }

            } else {

                if (feature_Multi) {
                    if (cont == 1) {
                        var fileName = 'ART2' + '_' + companyruc + '_' + AAAA + '_' + param_Subsi + '_' + param_Multi + '.txt';
                    } else {
                        var fileName = 'ART2' + '_' + companyruc + '_' + AAAA + '_' + param_Subsi + '_' + param_Multi + '_' + cont + '.txt';
                    }
                } else {
                    if (cont == 1) {
                        var fileName = 'ART2' + '_' + companyruc + '_' + AAAA + '_' + param_Subsi + '.txt';
                    } else {
                        var fileName = 'ART2' + '_' + companyruc + '_' + AAAA + '_' + param_Subsi + '_' + cont + '.txt';
                    }
                }

            }


            // Almacena en la carpeta de Archivos Generados
            if (folderId != '' && folderId != null) {
                // Extension del archivo
                // Crea el archivo
                if (param_head == 'T' || param_head == true) {

                    var globalLabels = getGlobalLabels();
                    var titulo = globalLabels.cabecera[language];

                    strAuxiliar = titulo + strAuxiliar;
                    log.debug('strAuxiliar', strAuxiliar);

                    var percepcionFile = fileModulo.create({
                        name: fileName,
                        fileType: fileModulo.Type.CSV,
                        contents: strAuxiliar,
                        encoding: fileModulo.Encoding.UTF8,
                        folder: folderId
                    });
                } else {
                    var percepcionFile = fileModulo.create({
                        name: fileName,
                        fileType: fileModulo.Type.PLAINTEXT,
                        contents: strAuxiliar,
                        encoding: fileModulo.Encoding.UTF8,
                        folder: folderId
                    });
                }


                var idfile = percepcionFile.save(); // Termina de grabar el archivo
                param_IDFiles = idfile;
                log.debug({
                    title: 'param_IDFiles',
                    details: param_IDFiles
                });
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

                if (idfile) {
                    var usuario = runtime.getCurrentUser();
                    var employee = search.lookupFields({
                        type: search.Type.EMPLOYEE,
                        id: usuario.id,
                        columns: ['firstname', 'lastname']
                    });
                    var usuarioName = employee.firstname + ' ' + employee.lastname;

                    if (cont > 1) {
                        var recordLog = recordModulo.create({
                            type: 'customrecord_lmry_co_rpt_generator_log'
                        });

                        //Nombre de Archivo
                        recordLog.setValue({
                            fieldId: 'custrecord_lmry_co_rg_name',
                            value: fileName
                        });

                        //Url de Archivo
                        recordLog.setValue({
                            fieldId: 'custrecord_lmry_co_rg_url_file',
                            value: urlfile
                        });

                        //Nombre de Reporte
                        recordLog.setValue({
                            fieldId: 'custrecord_lmry_co_rg_transaction',
                            value: reportName
                        });

                        //Nombre de Subsidiaria
                        recordLog.setValue({
                            fieldId: 'custrecord_lmry_co_rg_subsidiary',
                            value: companyname
                        });

                        //Periodo
                        recordLog.setValue({
                            fieldId: 'custrecord_lmry_co_rg_postingperiod',
                            value: periodname
                        });

                        //Multibook
                        recordLog.setValue({
                            fieldId: 'custrecord_lmry_co_rg_multibook',
                            value: multibookName
                        });

                        //Creado Por
                        recordLog.setValue({
                            fieldId: 'custrecord_lmry_co_rg_employee',
                            value: usuarioName
                        });

                        recordLog.save();
                    } else {
                        var recordLog = recordModulo.load({
                            type: 'customrecord_lmry_co_rpt_generator_log',
                            id: param_RecorID
                        });

                        //Periodo
                        recordLog.setValue({
                            fieldId: 'custrecord_lmry_co_rg_postingperiod',
                            value: periodname
                        });

                        //Nombre de Archivo
                        recordLog.setValue({
                            fieldId: 'custrecord_lmry_co_rg_name',
                            value: fileName
                        });

                        //Url de Archivo
                        recordLog.setValue({
                            fieldId: 'custrecord_lmry_co_rg_url_file',
                            value: urlfile
                        });

                        //Creado Por
                        recordLog.setValue({
                            fieldId: 'custrecord_lmry_co_rg_employee',
                            value: usuarioName
                        });

                        recordLog.save();

                    }

                }

            } else {
                // Debug
                log.debug({
                    title: 'Creacion de File:',
                    details: 'No existe el folder'
                });
            }

        }

        function Remplaza_tildes(s) {
            var AccChars = "ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðñòóôõöùúûüýÿ°Ñ–—";
            var RegChars = "SZszYAAAAAACEEEEIIIIDNOOOOOUUUUYaaaaaaceeeeiiiidnooooouuuuyyoN--";
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
            var AccChars = "-./(),;''_..-";
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
            var AccChars = '.!“#$%&/()=\\.-·+/*ªº"".,;ªº-._[].·';
            var RegChars = "                                  ";
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

        function DatosCustomer(id_customer) {
            var datos = search.create({
                type: "vendor",
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
                        name: "custrecord_lmry_addr_city_id",
                        join: "billingAddress",
                        label: "Latam - City ID"
                    }),
                    search.createColumn({
                        name: "custrecord_lmry_addr_prov_id",
                        join: "billingAddress",
                        label: "Latam - Province ID"
                    })
                ]
            });

            var resultado = datos.run().getRange(0, 1000);
            //log.debug('resultado del vendor direccion',resultado);
            if (resultado.length != 0) {
                var columns = resultado[0].columns;

                direccion = resultado[0].getValue(columns[0]) + ' ' + resultado[0].getValue(columns[1]);
                direccion = direccion.substring(0, 70);
                direccion = Valida_caracteres_blanco(direccion)
                //direccion= QuitaGuion(direccion);
                direccion = Remplaza_tildes(direccion);
                municipio = resultado[0].getValue(columns[2]).substring(0, 5);
                //log.debug('municipio',municipio);
                departamento = resultado[0].getValue(columns[3]).substring(0, 2);
                //log.debug('departamnto',departamento);
            } else {
                direccion = '';
                municipio = '';
                departamento = '';
            }


            return direccion + '|' + municipio + '|' + departamento;

        }

        function getParameterAndFeatures() {
            var objContext = runtime.getCurrentScript();

            // Parámetros
            param_RecorID = objContext.getParameter({
                name: 'custscript_lmry_co_art2_recid'
            });
            param_Anual = objContext.getParameter({
                name: 'custscript_lmry_co_art2_anual'
            });
            param_Multi = objContext.getParameter({
                name: 'custscript_lmry_co_art2_mutibook'
            });
            param_FeatID = objContext.getParameter({
                name: 'custscript_lmry_co_art2_featid'
            });
            param_Subsi = objContext.getParameter({
                name: 'custscript_lmry_co_art2_subsi'
            });
            param_head = objContext.getParameter({
                name: 'custscript_lmry_co_art2_inserthead'
            });

            // Features
            feature_Subsi = runtime.isFeatureInEffect({
                feature: "SUBSIDIARIES"
            });
            feature_Multi = runtime.isFeatureInEffect({
                feature: "MULTIBOOK"
            });
            isMultiCalendar = runtime.isFeatureInEffect({
                feature: 'MULTIPLECALENDARS'
            });

            log.debug('Parámetros', param_RecorID + '---' + param_Anual + '---' + param_Multi + '---' + param_FeatID + '---' + param_Subsi + '---' + param_head);

            /** DESVINCULACION */
            featureSpecialPeriod = getFeatures(677);
            log.debug('Hay Special', featureSpecialPeriod);
            if (featureSpecialPeriod == true || featureSpecialPeriod == 'T') {
                //Period Name
                periodname = param_Anual;

                //Period Start Date
                periodstartdate = '01/01/' + param_Anual;
                //Period End Date
                periodenddate = '31/12/' + param_Anual;

                //Obteniendo los IDs de los periodos del record Special Accounting Periods
                var specialPeriodsIDs = getSpecialPeriods(param_Anual);
                if (specialPeriodsIDs.length != 0) {
                    //Armando la fórmula de periodos
                    formulPeriodFilters = generarStringFilterPostingPeriodAnual(specialPeriodsIDs);
                }
            } else {
                //Period name, start date y enddate
                var periodenddate_temp = search.lookupFields({
                    type: search.Type.ACCOUNTING_PERIOD,
                    id: param_Anual,
                    columns: ['enddate', 'periodname', 'startdate']
                });

                periodenddate = periodenddate_temp.enddate;
                periodstartdate = periodenddate_temp.startdate;
                //Period Name
                periodname = periodenddate_temp.periodname;
                var yearPeriod = periodname.split(' ');
                periodname = yearPeriod[1];

                // Obtener Filtro de fecha
                var arregloidPeriod = getPeriods(periodstartdate, periodenddate);
                if (arregloidPeriod.length != 0) {
                    //Armando la fórmula de periodos
                    formulPeriodFilters = generarStringFilterPostingPeriodAnual(arregloidPeriod);
                }
            }

            Anual = periodname;

        }

        function getTransactions() {
            var intDMinReg = 0;
            var intDMaxReg = 1000;
            var DbolStop = false;
            //para la busqueda de transacciones
            var ArrReturn = [];

            var savedsearch = search.load({
                id: 'customsearch_lmry_co_art_2'
            });

            if (feature_Subsi) {
                var subsidiaryFilter = search.createFilter({
                    name: 'subsidiary',
                    operator: search.Operator.IS,
                    values: [param_Subsi]
                });
                savedsearch.filters.push(subsidiaryFilter);
            }

            // Filtro Fórmula (Text) con los IDs de los periodos
            log.debug('formulPeriodFilters', formulPeriodFilters);
            var periodFilter = search.createFilter({
                name: "formulatext",
                formula: formulPeriodFilters,
                operator: search.Operator.IS,
                values: "1"
            });
            savedsearch.filters.push(periodFilter);

            var paymentconcept = search.createFilter({
                name: 'custbody_lmry_payment_type',
                operator: search.Operator.NONEOF,
                values: "@NONE@"
            });
            savedsearch.filters.push(paymentconcept);

            //columna13
            var columnaExchangeRate = search.createColumn({
                name: 'exchangerate',
                summary: 'Group',
                label: "Exchange Rate"
            });
            savedsearch.columns.push(columnaExchangeRate);


            if (feature_Multi) {
                var multibookFilter = search.createFilter({
                    name: 'accountingbook',
                    join: 'accountingtransaction',
                    operator: search.Operator.IS,
                    values: [param_Multi]
                });
                savedsearch.filters.push(multibookFilter);

                //columna14
                var columnaExchangeRateMulti = search.createColumn({
                    name: 'exchangerate',
                    summary: 'Group',
                    join: "accountingTransaction",
                    label: "Exchange Rate"
                });
                savedsearch.columns.push(columnaExchangeRateMulti);
            }

            var searchResult = savedsearch.run();

            while (!DbolStop) {
                var objResult = searchResult.getRange(intDMinReg, intDMaxReg);
                if (objResult != null) {
                    if (objResult.length != 1000) {
                        DbolStop = true;
                    }

                    for (var i = 0; i < objResult.length; i++) {
                        var columns = objResult[i].columns;
                        var arrAuxiliar = [];
                        //monto a comparar
                        if (feature_Multi) {
                            var UVT_3500 = objResult[i].getValue(columns[8]) * objResult[i].getValue(columns[14]) / objResult[i].getValue(columns[13]);
                        } else {
                            var UVT_3500 = objResult[i].getValue(columns[8]);

                        }

                        if ((UVT_3500 > 5000000) || (UVT_3500 == 5000000)) {

                            // 0. id vendor
                            if (objResult[i].getValue(columns[0]) != '' && objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -') {
                                arrAuxiliar[0] = objResult[i].getValue(columns[0]);
                            } else {
                                arrAuxiliar[0] = '';
                            }

                            //1.tipo de documento
                            if (objResult[i].getValue(columns[1]) != '' && objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -') {
                                arrAuxiliar[1] = objResult[i].getValue(columns[1]);
                            } else {
                                arrAuxiliar[1] = '';
                            }
                            //2. numero de documento
                            if (objResult[i].getValue(columns[2]) != '' && objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -') {
                                arrAuxiliar[2] = objResult[i].getValue(columns[2]);
                            } else {
                                arrAuxiliar[2] = '';
                            }
                            //3. nombre o razon social
                            if (objResult[i].getValue(columns[3]) != '' && objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -') {
                                arrAuxiliar[3] = objResult[i].getValue(columns[3]);
                            } else {
                                arrAuxiliar[3] = '';
                            }
                            //4. direccion
                            if (objResult[i].getValue(columns[4]) != '' && objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -') {
                                arrAuxiliar[4] = objResult[i].getValue(columns[4]);
                            } else {
                                arrAuxiliar[4] = '';
                            }
                            //5. telefono
                            if (objResult[i].getValue(columns[5]) != '' && objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -') {
                                arrAuxiliar[5] = objResult[i].getValue(columns[5]);
                            } else {
                                arrAuxiliar[5] = '';
                            }
                            //6. email
                            if (objResult[i].getValue(columns[6]) != '' && objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -') {
                                arrAuxiliar[6] = objResult[i].getValue(columns[6]);
                            } else {
                                arrAuxiliar[6] = '';
                            }
                            //7. concepto de pago
                            if (objResult[i].getValue(columns[7]) != '' && objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '- None -') {
                                arrAuxiliar[7] = objResult[i].getValue(columns[7]);
                            } else {
                                arrAuxiliar[7] = '';
                            }
                            //8. valor compra anual
                            if (feature_Multi) {
                                if (objResult[i].getValue(columns[10]) != 'vendorbill') {
                                    if (objResult[i].getValue(columns[8]) != '' && objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -') {
                                        arrAuxiliar[8] = objResult[i].getValue(columns[8]) * objResult[i].getValue(columns[14]) / objResult[i].getValue(columns[13]);
                                    } else {
                                        arrAuxiliar[8] = 0;
                                    }
                                } else {
                                    arrAuxiliar[8] = 0;
                                }
                            } else {
                                if (objResult[i].getValue(columns[10]) != 'vendorbill') {
                                    if (objResult[i].getValue(columns[8]) != '' && objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -') {
                                        arrAuxiliar[8] = objResult[i].getValue(columns[8]);
                                    } else {
                                        arrAuxiliar[8] = 0;
                                    }
                                } else {
                                    arrAuxiliar[8] = 0;
                                }
                            }

                            //9. valor devoluciones

                            if (feature_Multi) {
                                if (objResult[i].getValue(columns[10]) == 'vendorbill') {
                                    arrAuxiliar[9] = objResult[i].getValue(columns[8]) * objResult[i].getValue(columns[14]) / objResult[i].getValue(columns[13]);
                                } else {
                                    arrAuxiliar[9] = 0;
                                }
                            } else {
                                if (objResult[i].getValue(columns[10]) == 'vendorbill') {
                                    arrAuxiliar[9] = objResult[i].getValue(columns[8]);
                                } else {
                                    arrAuxiliar[9] = 0;
                                }
                            }

                            //10. vigencia
                            arrAuxiliar[10] = Anual;
                            //11. id de la transaccion
                            arrAuxiliar[11] = objResult[i].getValue(columns[11]);
                            //12. id del co transaccion fields
                            if (objResult[i].getValue(columns[12]) != '' && objResult[i].getValue(columns[12]) != null && objResult[i].getValue(columns[12]) != '- None -') {
                                arrAuxiliar[12] = objResult[i].getValue(columns[12]);
                            } else {
                                arrAuxiliar[12] = '';
                            }
                            //13. tipo de transaccion
                            arrAuxiliar[13] = objResult[i].getValue(columns[10]);

                            ArrReturn.push(arrAuxiliar);
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

            log.debug('dta Busqueda principal', ArrReturn);
            return ArrReturn;
        }

        function getFeatures(idFeature) {
            var isActivate = false;
            var licenses = new Array();

            licenses = libreriaFeature.getLicenses(param_Subsi);
            isActivate = libreriaFeature.getAuthorization(idFeature, licenses);

            return isActivate;
        }

        function getPeriods(startDateAux, endDateAux) {
            var period = new Array();
            var varFilter = new Array();
            if (isMultiCalendar) {
                var varSubsidiary = search.lookupFields({
                    type: 'subsidiary',
                    id: param_Subsi,
                    columns: ['fiscalcalendar']
                });
                var fiscalCalendar = varSubsidiary.fiscalcalendar[0].value;
                var accountingperiodObj = search.create({
                    type: 'accountingperiod',
                    filters: [
                        ['isyear', 'is', 'F'],
                        'AND',
                        ['isquarter', 'is', 'F'],
                        'AND',
                        ['isadjust', 'is', 'F'],
                        'AND',
                        ['fiscalcalendar', 'anyof', fiscalCalendar],
                        'AND',
                        ['startdate', 'onorafter', startDateAux],
                        'AND',
                        ['enddate', 'onorbefore', endDateAux]
                    ],
                    columns: [
                        search.createColumn({
                            name: "periodname",
                            summary: "GROUP",
                            label: "Name"
                        }),
                        search.createColumn({
                            name: "startdate",
                            summary: "GROUP",
                            sort: search.Sort.ASC,
                            label: "Start Date"
                        }),
                        search.createColumn({
                            name: "enddate",
                            summary: "GROUP",
                            label: "End Date"
                        }),
                        search.createColumn({
                            name: "internalid",
                            summary: "GROUP",
                            label: "Internal ID"
                        })
                    ]
                });
            } else {
                var accountingperiodObj = search.create({
                    type: 'accountingperiod',
                    filters: [
                        ['isyear', 'is', 'F'],
                        'AND',
                        ['isquarter', 'is', 'F'],
                        'AND',
                        ['isadjust', 'is', 'F'],
                        'AND',
                        ['startdate', 'onorafter', startDateAux],
                        'AND',
                        ['enddate', 'onorbefore', endDateAux]
                    ],
                    columns: [
                        search.createColumn({
                            name: "periodname",
                            summary: "GROUP",
                            label: "Name"
                        }),
                        search.createColumn({
                            name: "startdate",
                            summary: "GROUP",
                            sort: search.Sort.ASC,
                            label: "Start Date"
                        }),
                        search.createColumn({
                            name: "enddate",
                            summary: "GROUP",
                            label: "End Date"
                        }),
                        search.createColumn({
                            name: "internalid",
                            summary: "GROUP",
                            label: "Internal ID"
                        })
                    ]
                });
            }

            // Ejecutando la busqueda
            var varResult = accountingperiodObj.run();
            var AccountingPeriodRpt = varResult.getRange({
                start: 0,
                end: 1000
            });
            if (AccountingPeriodRpt == null || AccountingPeriodRpt.length == 0) {
                log.debug('NO DATA', 'No hay periodos para ese año seleccionado');
                return false;
            } else {
                var columns;
                for (var i = 0; i < AccountingPeriodRpt.length; i++) {
                    columns = AccountingPeriodRpt[i].columns;
                    period[i] = new Array();
                    period[i] = AccountingPeriodRpt[i].getValue(columns[3]);
                }
            }

            return period;
        }

        function getSpecialPeriods(year) {
            var specialPeriods_ID = [];
            var searchPeriodSpecial = search.create({
                type: "customrecord_lmry_special_accountperiod",
                filters: [
                    ["isinactive", "is", "F"],
                    "AND",
                    ["custrecord_lmry_anio_fisco", "is", year],
                    "AND",
                    ["custrecord_lmry_adjustment", "is", "F"]
                ],
                columns: [
                    search.createColumn({
                        name: "custrecord_lmry_accounting_period"
                    })
                ]
            });

            if (isMultiCalendar == true || isMultiCalendar == 'T') {

                var subsiCalendar = search.lookupFields({
                    type: search.Type.SUBSIDIARY,
                    id: param_Subsi,
                    columns: ['fiscalcalendar']
                });

                calendarSub = {
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
                    specialPeriods_ID.push(searchResult[i].getValue(columns[0]));
                }
            } else {
                log.error('Alerta', 'No existe periodos configurados en el record Special Periods, se tomaron los periodos normales del accounting period de Netsuite que esten entre ' +
                    'el 1 de Enero y el 31 Diciembre del año seleccionado.');

                var arregloidPeriod = getPeriods(periodstartdate, periodenddate);
                formulPeriodFilters = generarStringFilterPostingPeriodAnual(arregloidPeriod);
            }

            return specialPeriods_ID;
        }

        function generarStringFilterPostingPeriodAnual(idsPeriod) {
            var cant = idsPeriod.length;
            var comSimpl = "'";
            var strinic = "CASE WHEN ({postingperiod.id}=" + comSimpl + idsPeriod[0] + comSimpl;
            var strAdicionales = "";
            var strfinal = ") THEN 1 ELSE 0 END";
            for (var i = 1; i < cant; i++) {
                strAdicionales += " or {postingperiod.id}=" + comSimpl + idsPeriod[i] + comSimpl;
            }
            var str = strinic + strAdicionales + strfinal;
            return str;
        }

        function getGlobalLabels() {
            var labels = {
                cabecera: {
                    en: 'VALIDITY' + ',' + 'DOCUMENT TYPE' + ',' + 'DOCUMENT NUMBER' + ',' + 'NAME OR SOCIAL REASON' + ',' + 'NOTIFICATION ADDRESS' + ',' + 'PHONE' + ',' + 'EMAIL' + ',' + 'MUNICIPALITY CODE' + ',' + 'DEPARTMENT CODE' + ',' + 'ITEM PAYMENT OR CREDIT IN ACCOUNT' + ',' + 'ANNUAL PURCHASES VALUE' + ',' + 'RETURN VALUE' + '\r\n',
                    es: 'VIGENCIA' + ',' + 'TIPO DE DOCUMENTO' + ',' + 'NUMERO DE DOCUMENTO' + ',' + 'NOMBRE O RAZON SOCIAL' + ',' + 'DIRECCION DE NOTIFICACION' + ',' + 'TELEFONO' + ',' + 'EMAIL' + ',' + 'CODIGO DE MUNICIPIO' + ',' + 'CODIGO DE DEPARTAMENTO' + ',' + 'CONCEPTO PAGO O ABONO EN CUENTA' + ',' + 'VALOR COMPRAS ANUAL' + ',' + 'VALOR DEVOLUCIONES' + '\r\n',
                    pt: 'VALIDADE' + ',' + 'TIPO DE DOCUMENTO' + ',' + 'NUMERO DO DOCUMENTO' + ',' + 'NOME OU NOME DA EMPRESA' + ',' + 'ENDERECO DE NOTIFICACAO' + ',' + 'TELEFONE' + ',' + 'O EMAIL' + ',' + 'CODIGO DO MUNICIPIO' + ',' + 'CODIGO DO DEPARTAMENTO' + ',' + 'PAGAMENTO DO ITEM OU CREDITO EM CONTA' + ',' + 'VALOR DE COMPRAS ANUAIS' + ',' + 'VALOR DE RETORNO' + '\r\n'
                }
            }

            return labels;
        }

        return {
            getInputData: getInputData,
            map: map,
            summarize: summarize
        };

    });