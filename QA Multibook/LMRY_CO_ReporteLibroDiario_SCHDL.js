/**
 * Module Description
 * 
 * Version    Date            Author           Remarks
 * 1.00       04 Dec 2014     LatamReady Consultor
 * File: LMRY_CO_ReporteLibroDiario_SCHDL.js
 */
var objContext =  nlapiGetContext();
// Nombre del Reporte
var namereport = "Reporte de Libro Diario";
var LMRY_script = 'LMRY CO Reportes Libro Diario SCHDL';
// Control de Memoria
var intMaxReg = 1000;
var intMinReg = 0;
var bolStop = false;
var intTotalRows = 0;
var intLoops = 1; 
//Parametros
var paramsubsidi = ''; 
var paramperiodo  = '';
//Control de Reporte
var periodstartdate = '';
var periodenddate = '';
var configpage = '';
var companyruc = '';
var companyname = ''; 
var xlsString = '';
var ArrLibroDiario = new Array();
var ArrQuiebreFecha = new Array();
var ArrQuiebreCuenta = new Array();
var strName = '';
var periodname = '';
var auxmess = '';
var auxanio = '';
var	paramMultibook	=	null;	


//Valida si es OneWorld
var featuresubs	 =	objContext.getFeature('SUBSIDIARIES');
var feamultibook =	objContext.getFeature('MULTIBOOK');


/* ***********************************************
 * Arreglo con la structura de la tabla log
 * ******************************************** */ 
var RecordName  = 'customrecord_lmry_co_rpt_generator_log';
var RecordTable = ['custrecord_lmry_co_rg_name' , 
                   'custrecord_lmry_co_rg_postingperiod' , 
                   'custrecord_lmry_co_rg_subsidiary',
                   'custrecord_lmry_co_rg_url_file',
                   'custrecord_lmry_co_rg_employee'];

/* ***********************************************
 * Inicia el proceso Schedule
 * ******************************************** */ 
function scheduled_main_LMRYLibroDiario(type) {
	
	//variables
	var _FechaQuiebre = '';
	var _TotalDebFechaQuiebre = 0.00;
	var _TotalCreFechaQuiebre = 0.00;
	var _TotalDebCuentaQuiebre = 0.00;
	var _TotalCreCuentaQuiebre = 0.00;
	var _TotalDebGeneral = 0.00;
	var _TotalCreGeneral = 0.00;
	var _CtaQuiebre = '';
	
	
	// Parametros
	paramsubsidi = objContext.getSetting('SCRIPT', 'custscript_lmry_subsidi_librodiarioco');
	paramperiodo = objContext.getSetting('SCRIPT', 'custscript_lmry_periodo_librodiarioco');
	paramidrpt = objContext.getSetting('SCRIPT', 'custscript_lmry_idrpt_librodiarioco');
	paramMultibook	=	objContext.getSetting('SCRIPT', 'custscript_lmry_multibook_librodiarioco');
	
	nlapiLogExecution('ERROR', 'paramsubsidi, paramperiodo, paramidrpt-> ', paramsubsidi + ', ' + paramperiodo + ', ' + paramidrpt);
	
	// Datos de la empresa
	configpage = nlapiLoadConfiguration('companyinformation');
	companyruc = configpage.getFieldValue('employerid');
	companyname = configpage.getFieldValue('companyname');

	var featuresubs = nlapiGetContext().getFeature('SUBSIDIARIES');
	if (featuresubs == true) {
		companyname = ObtainNameSubsidiaria(paramsubsidi);	
		companyruc = ObtainFederalIdSubsidiaria(paramsubsidi);
	}
	
	if (paramperiodo!=null && paramperiodo!=''){
	    var columnFrom = nlapiLookupField('accountingperiod', paramperiodo, ['enddate','periodname','startdate']);
	    periodstartdate = columnFrom.startdate;
	    periodenddate = columnFrom.enddate;
	    periodname = columnFrom.periodname;
	}
	
	ObtieneQuiebreFecha();
	ObtieneQuiebreFechaCuenta();
	ObtieneLibroDiarioCO();
	
	//cabecera de excel
	xlsString = '<?xml version="1.0" encoding="UTF-8" ?><?mso-application progid="Excel.Sheet"?>'; 
	xlsString += '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" ';
	xlsString += 'xmlns:o="urn:schemas-microsoft-com:office:office" ';
	xlsString += 'xmlns:x="urn:schemas-microsoft-com:office:excel" ';
	xlsString += 'xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet" ';
	xlsString += 'xmlns:html="http://www.w3.org/TR/REC-html40">'; 
	
	// Propiedades del Documento
	xlsString += '<DocumentProperties xmlns="urn:schemas-microsoft-com:office:office">'; 
		xlsString += '<Author>' + companyname + '</Author>'; 
		xlsString += '<LastAuthor>' + companyname + '</LastAuthor>'; 
		xlsString += '<Created></Created>'; 
		xlsString += '<Company>' + companyname + '</Company>'; 
		xlsString += '<Version>2016.1.1</Version>'; 
	xlsString += '</DocumentProperties>'; 

	// Estilos de celdas
	xlsString += '<Styles>';
		xlsString += '<Style ss:ID="s20"><Font ss:Bold="1"/><Alignment ss:Horizontal="Center" ss:Vertical="Bottom"/></Style>';
		xlsString += '<Style ss:ID="s21"><Font ss:Bold="1" ss:Size="12" /><Alignment ss:Horizontal="Center" ss:Vertical="Bottom"/></Style>';
		xlsString += '<Style ss:ID="s22"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom" ss:WrapText="1"/></Style>';
		xlsString += '<Style ss:ID="s23"><Font ss:Bold="1"/><Alignment ss:Horizontal="Right" ss:Vertical="Bottom"/></Style>';
		xlsString += '<Style ss:ID="s24"><NumberFormat ss:Format="_(* #,##0.00_);_(* \(#,##0.00\);_(* &quot;-&quot;??_);_(@_)"/></Style>';
	xlsString += '</Styles>';

	// Nombre de la hoja
	xlsString += '<Worksheet ss:Name="Libro Diario">';
		
	xlsString += '<Table>' ;
	xlsString += '<Column ss:AutoFitWidth="0" ss:Width="060"/>';
	xlsString += '<Column ss:AutoFitWidth="0" ss:Width="220"/>';
	xlsString += '<Column ss:AutoFitWidth="0" ss:Width="140"/>';
	xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
	xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
	//Cabecera
	xlsString += '<Row>';
	xlsString += '<Cell></Cell>' ;
	xlsString += '<Cell></Cell>' ;
	xlsString += '<Cell ss:StyleID="s21"><Data ss:Type="String"> LIBRO DIARIO </Data></Cell>' ;
	xlsString += '</Row>';
	xlsString += '<Row></Row>';
	xlsString += '<Row>';
	xlsString += '<Cell></Cell>' ;
	xlsString += '<Cell></Cell>' ;
	xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">Razon Social: '+ companyname +'</Data></Cell>' ;
	xlsString += '</Row>';
	xlsString += '<Row>';
	xlsString += '<Cell></Cell>' ;
	xlsString += '<Cell></Cell>' ;
	xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">NIT: '+ companyruc +'</Data></Cell>' ;
	xlsString += '</Row>';
	xlsString += '<Row>';
	xlsString += '<Cell></Cell>'; 
	xlsString += '<Cell></Cell>' ;
	xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">Periodo: ' + periodstartdate + ' al ' +  periodenddate + '</Data></Cell>' ;
	xlsString += '</Row>';	
	// Una linea en blanco
	xlsString += '<Row></Row>';
	// Titulo de las columnas
	xlsString += '<Row></Row>';
	xlsString += 	'<Row>' +
						'<Cell ss:StyleID="s21"><Data ss:Type="String"> Cuenta </Data></Cell>' +
						'<Cell ss:StyleID="s21"><Data ss:Type="String"> Denominacion </Data></Cell>' +
						'<Cell ss:StyleID="s21"><Data ss:Type="String"> Documento </Data></Cell>' +
						'<Cell ss:StyleID="s21"><Data ss:Type="String"> Debito </Data></Cell>' +
						'<Cell ss:StyleID="s21"><Data ss:Type="String"> Credito </Data></Cell>' +
					'</Row>';
	
	//creacion de reporte xls
	for(var i=0; i<=ArrQuiebreFecha.length-1; i++)
	 {
		_FechaQuiebre = ArrQuiebreFecha[i][0];
		_TotalDebFechaQuiebre  = ArrQuiebreFecha[i][1];
		_TotalCreFechaQuiebre  = ArrQuiebreFecha[i][2];
		
	  	// arma el primer quiebre
	  	xlsString += '<Row>';
	  	xlsString += '<Cell></Cell>' ;
	  	xlsString += '<Cell></Cell>' ;
	  	// MergeAcross = "1" (Junta dos columnas) "2" (Junta tres columnas), junta una mas del parametro que se pasa
	    xlsString += '<Cell ss:StyleID="s22" ss:MergeAcross="1"><Data ss:Type="String">Movimientos del dia '+ _FechaQuiebre +'</Data></Cell>' ;  
	  	xlsString += '</Row>';
		
	  	_CtaQuiebre ='';
	  	_TotalDebCuentaQuiebre = 0.00;
	  	_TotalCreCuentaQuiebre = 0.00;
	  	for(var ii=0; ii<=ArrQuiebreCuenta.length-1; ii++)
			{
			  if (_FechaQuiebre == ArrQuiebreCuenta[ii][0])
			    {
				  	_CtaQuiebre = ArrQuiebreCuenta[ii][1];
					_TotalDebCuentaQuiebre  = parseFloat(ArrQuiebreCuenta[ii][3]);
					_TotalCreCuentaQuiebre  = parseFloat(ArrQuiebreCuenta[ii][4]);
					
				  	//arma el segungo quiebre
				  	//xlsString += '<Row>';
  					//xlsString += '</Row>';
				  	
  					for(var xi=0; xi<=ArrLibroDiario.length-1; xi++)
				  		{
							// fecha y cuenta
				  			if ((_FechaQuiebre==ArrLibroDiario[xi][0]) && (_CtaQuiebre==ArrLibroDiario[xi][1]))
				  				{
				  					//arma Detalle del reporte
				  					xlsString += '<Row>';
									//1. Numero de Cuenta
									if (ArrQuiebreCuenta[ii][1]!='' || ArrQuiebreCuenta[ii][1]!=null)
										{ xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">'+ ArrQuiebreCuenta[ii][1] +'</Data></Cell>' ;}
									else
										{ xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String"></Data></Cell>' ;  }
									//2. Denominaci?n  					
									if (ArrQuiebreCuenta[ii][2].length > 0) //(ArrQuiebreCuenta[ii][2]!='' || ArrQuiebreCuenta[ii][2]!=null)
										{ xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">'+ ArrQuiebreCuenta[ii][2] +'</Data></Cell>' ;}
									else
										{ xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String"></Data></Cell>' ;  }
				  					//3. Documento
				  					if (ArrLibroDiario[xi][3]!='' || ArrLibroDiario[xi][3]!=null)
				  						{ xlsString += '<Cell><Data ss:Type="String">'+ ArrLibroDiario[xi][3] +'</Data></Cell>' ;}
				  					else
				  						{ xlsString += '<Cell><Data ss:Type="String"></Data></Cell>' ;  }
				  					//4. Suma Debito
				  					if (ArrLibroDiario[xi][4]!='' || ArrLibroDiario[xi][4]!=null)
				  						{ 
				  						xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">'+ ArrLibroDiario[xi][4] +'</Data></Cell>' ;
				  						_TotalDebGeneral += parseFloat(ArrLibroDiario[xi][4]);
				  						}
				  					else
				  						{ xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">0.00</Data></Cell>' ;  }
				  					//5. Suma Credito
				  					if (ArrLibroDiario[xi][5]!='' || ArrLibroDiario[xi][5]!=null)
				  						{ 
				  						xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">'+ ArrLibroDiario[xi][5] +'</Data></Cell>' ;
				  						_TotalCreGeneral += parseFloat(ArrLibroDiario[xi][5]);
				  						}
				  					else
				  						{ xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">0.00</Data></Cell>' ;  }
				  					xlsString += '</Row>';
				  					
				  				}
							// arma el total del segundo quiebre
							/* // Se comento totales por cuenta
							xlsString += '<Row>';
							xlsString += '<Cell></Cell>';
							xlsString += '<Cell></Cell>';
							xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">Total</Data></Cell>' ;
							xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">'+ parseFloat(_TotalDebCuentaQuiebre).toFixed(2) +'</Data></Cell>' ;
							xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">'+ parseFloat(_TotalCreCuentaQuiebre).toFixed(2) +'</Data></Cell>' ;
							xlsString += '</Row>';
							xlsString += '<Row></Row>';
							*/									
				  		} //fin de detalle del reporte  					
				}// fin del segundo quiebre
			}// fin del for segundo quiebre
		
		// arma el total del primer quiebre
	  	xlsString += '<Row>';
		  	xlsString += '<Cell></Cell>';
		  	xlsString += '<Cell ss:StyleID="s23" ss:MergeAcross="1"><Data ss:Type="String">Total movimientos del dia '+ _FechaQuiebre +'</Data></Cell>' ;
		    xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">'+ parseFloat(_TotalDebFechaQuiebre).toFixed(2) +'</Data></Cell>' ;
		    xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">'+ parseFloat(_TotalCreFechaQuiebre).toFixed(2) +'</Data></Cell>' ;
	  	xlsString += '</Row>';
	 }
	
	//arma el total del primer quiebre
  	xlsString += '<Row>';
	    xlsString += '<Cell></Cell>';
	    xlsString += '<Cell ss:MergeAcross="1" ss:StyleID="s23"><Data ss:Type="String">Total movimientos del periodo ' + periodstartdate + ' al ' +  periodenddate + '</Data></Cell>' ;
	    xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">'+ parseFloat(_TotalDebGeneral).toFixed(2)  +'</Data></Cell>' ;
	    xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">'+ parseFloat(_TotalCreGeneral).toFixed(2) +'</Data></Cell>' ;
  	xlsString += '</Row>';
  	
	// Cierra la tabla
	xlsString += '</Table>';
	
	// Cierra la Hoja de Trabajo 1
	xlsString += '</Worksheet>';
		
	// Cierra el Libro
	xlsString += '</Workbook>';
	
	Periodo(periodname);
	
	//Se arma el archivo EXCEL
	strName = nlapiEncrypt(xlsString, 'base64');
	var NameFile  = "COLibroDiario_"+companyname+"_"+auxmess+"_"+auxanio+".xls";
	savefile(NameFile,'EXCEL');
}

function strImporte(importe) {
	nlapiLogExecution('ERROR', 'importe-> ', importe);
	var auximp = '' + importe;
	var pos = auximp.indexOf('.');
	if (pos==-1) {
	auximp = auximp + '.00';
	}
	nlapiLogExecution('ERROR', 'auximp-> ', auximp);
	return auximp;
	} 


function ObtieneQuiebreFechaCuenta()
{
	// Seteo de Porcentaje completo 
	objContext.setPercentComplete(0.00);
	
	// Control de Memoria
	var intDMaxReg = 1000;
	var intDMinReg = 0;
	var arrQuiebre = new Array();
    
	// Exedio las unidades
	var DbolStop = false;
	var usageRemaining = objContext.getRemainingUsage();

	// Valida si es OneWorld
	var featuresubs = nlapiGetContext().getFeature('SUBSIDIARIES');
    var _cont = 0;
    
    nlapiLogExecution('ERROR', 'parametros2-> ',paramsubsidi + ',' + paramperiodo);
	// Consulta de Cuentas
	var savedsearch = nlapiLoadSearch('transaction', 'customsearch_lmry_co_librodiariooficial');	
	// Valida si es OneWorld 
	if (featuresubs == true) {
		savedsearch.addFilter(new nlobjSearchFilter('subsidiary', null, 'is', paramsubsidi));
	}
	
	if ( (feamultibook == true || feamultibook == 'T') && (paramMultibook!='' && paramMultibook!=null) ){
		savedsearch.addFilter(new nlobjSearchFilter('accountingbook', 'accountingtransaction', 'anyof', paramMultibook));
	}
	savedsearch.addFilter(new nlobjSearchFilter('postingperiod', null, 'anyof', paramperiodo));
	
	var searchresult = savedsearch.runSearch();
	while(!DbolStop )	{
		var objResult = searchresult.getResults(intDMinReg, intDMaxReg);
		
		if (objResult != null)
		{
			var intLength = objResult.length;
			
			for(var i = 0; i < intLength; i++)
			 {
				columns = objResult[i].getAllColumns();
				arrQuiebre= new Array();
				//0. fecha
				if (objResult[i].getValue(columns[0])!=null)
					arrQuiebre[0] = objResult[i].getValue(columns[0]) ;	
				else
					arrQuiebre[0] = '';
				//1. cuenta
				if (objResult[i].getValue(columns[1])!=null)
					{ arrQuiebre[1] = objResult[i].getValue(columns[1]) ;  }						
				else
					arrQuiebre[1] = '';
				//2. denominacion
				if (objResult[i].getValue(columns[2])!=null)
					{ arrQuiebre[2]  = objResult[i].getValue(columns[2]) ; }
				else
					{ arrQuiebre[2] = ''; }
				//3. sum debitos
				if (objResult[i].getValue(columns[3])!=null)
					arrQuiebre[3] = parseFloat(objResult[i].getValue(columns[3])).toFixed(2);	
				else
					arrQuiebre[3] = 0.00;
				//4. sum creditos
				if (objResult[i].getValue(columns[4])!=null)
					arrQuiebre[4] = parseFloat(objResult[i].getValue(columns[4])).toFixed(2);	
				else
					arrQuiebre[4] = 0.00;
			
				ArrQuiebreCuenta[_cont] = arrQuiebre;
				_cont++;
			 
			 }
			 intDMinReg = intDMaxReg; 
			 intDMaxReg += 1000;
			 if (intLength<1000){
				 DbolStop = true;
			 }
		}
		else
		{ DbolStop = true;}
	}
//	nlapiLogExecution('ERROR', 'ArrQuiebreCuenta-> ',ArrQuiebreCuenta);
}


function ObtieneQuiebreFecha()
{
	// Seteo de Porcentaje completo 
	objContext.setPercentComplete(0.00);
	
	// Control de Memoria
	var intDMaxReg = 1000;
	var intDMinReg = 0;
	var arrQuiebre = new Array();
    
	// Exedio las unidades
	var DbolStop = false;
	var usageRemaining = objContext.getRemainingUsage();

	// Valida si es OneWorld
	var featuresubs = nlapiGetContext().getFeature('SUBSIDIARIES');
    var _cont = 0;

    nlapiLogExecution('ERROR', 'parametros1-> ',paramsubsidi + ',' + paramperiodo);
	// Consulta de Cuentas
	var savedsearch = nlapiLoadSearch('transaction', 'customsearch_lmry_co_librodiariototdia');	
	// Valida si es OneWorld 
	if (featuresubs == true) {
		savedsearch.addFilter(new nlobjSearchFilter('subsidiary', null, 'is', paramsubsidi));
	}
	savedsearch.addFilter(new nlobjSearchFilter('postingperiod', null, 'anyof', paramperiodo));
	
	var searchresult = savedsearch.runSearch();
	while(!DbolStop)	{
		var objResult = searchresult.getResults(intDMinReg, intDMaxReg);
		
		if (objResult != null)
		{
			var intLength = objResult.length;
			for(var i = 0; i < intLength; i++)
			 {
				columns = objResult[i].getAllColumns();
				arrQuiebre= new Array();
				
				//0. fecha
				if (objResult[i].getValue(columns[0])!=null)
					arrQuiebre[0] = objResult[i].getValue(columns[0]);	
				else
					arrQuiebre[0] = '';
				//1. sum debitos
				if (objResult[i].getValue(columns[1])!=null)
					arrQuiebre[1] = parseFloat(objResult[i].getValue(columns[1])).toFixed(2);	
				else
					arrQuiebre[1] = 0.00;
				//2. sum creditos
				if (objResult[i].getValue(columns[2])!=null)
					arrQuiebre[2] = parseFloat(objResult[i].getValue(columns[2])).toFixed(2);	
				else
					arrQuiebre[2] = 0.00;
			
				ArrQuiebreFecha[_cont] = arrQuiebre;
				_cont++;
			 
			 }
			 intDMinReg = intDMaxReg; 
			 intDMaxReg += 1000;
			 if (intLength<1000){
				 DbolStop = true;
			 }
		}
		else
		{ DbolStop = true;}
	}
//	nlapiLogExecution('ERROR', 'ArrQuiebreFecha.length-> ',ArrQuiebreFecha.length);

}

function ObtieneLibroDiarioCO()
{
	// Seteo de Porcentaje completo 
	objContext.setPercentComplete(0.00);
	
	// Control de Memoria
	var intDMaxReg = 1000;
	var intDMinReg = 0;
	var arrAuxiliar = new Array();
    
	// Exedio las unidades
	var DbolStop = false;
	var usageRemaining = objContext.getRemainingUsage();

	// Valida si es OneWorld
	var featuresubs = nlapiGetContext().getFeature('SUBSIDIARIES');
    var _cont = 0;
    
    //nlapiLogExecution('ERROR', 'parametros3-> ',paramsubsidi + ',' + paramperiodo);
	// Consulta de Cuentas
	var savedsearch = nlapiLoadSearch('transaction', 'customsearch_lmry_co_librodiario');	
	// Valida si es OneWorld 
	if (featuresubs == true) {
		savedsearch.addFilter(new nlobjSearchFilter('subsidiary', null, 'is', paramsubsidi));
	}
	savedsearch.addFilter(new nlobjSearchFilter('postingperiod', null, 'anyof', paramperiodo));

	var searchresult = savedsearch.runSearch();
	while(!DbolStop)	{
		var objResult = searchresult.getResults(intDMinReg, intDMaxReg);
		
		if (objResult != null)
		{
			var intLength = objResult.length;
			
			for(var i = 0; i < intLength; i++)
			 {
				columns = objResult[i].getAllColumns();
				arrAuxiliar = new Array();
				
				//0. fecha
				if (objResult[i].getValue(columns[0])!=null)
					arrAuxiliar[0] = objResult[i].getValue(columns[0]);	
				else
					arrAuxiliar[0] = '';
				//1. cuenta
				if (objResult[i].getValue(columns[1])!=null)
					arrAuxiliar[1] =  objResult[i].getValue(columns[1]);	
				else
					arrAuxiliar[1] = '';
				//2. denominacion
				if (objResult[i].getValue(columns[2])!=null)
					arrAuxiliar[2] = objResult[i].getValue(columns[2]);	
				else
					arrAuxiliar[2] = '';
				//3. documento
				if (objResult[i].getValue(columns[3])!=null)
					arrAuxiliar[3] = objResult[i].getText(columns[3]);	
				else
					arrAuxiliar[3] = '';
				//4. sum debitos
				if (objResult[i].getValue(columns[4])!=null)
					arrAuxiliar[4] = parseFloat(objResult[i].getValue(columns[4])).toFixed(2);	
				else
					arrAuxiliar[4] = 0.00;
				//4. sum credito
				if (objResult[i].getValue(columns[5])!=null)
					arrAuxiliar[5] = parseFloat(objResult[i].getValue(columns[5])).toFixed(2);	
				else
					arrAuxiliar[5] = 0.00;
			
				ArrLibroDiario[_cont] = arrAuxiliar;
				_cont++;
			 
			 }
			 intDMinReg = intDMaxReg; 
			 intDMaxReg += 1000;
			 if (intLength<1000){
				 DbolStop = true;
			 }
		}
		else
		{ DbolStop = true;}
	}
//	nlapiLogExecution('ERROR', 'ArrLibroDiario-> ',ArrLibroDiario);
}

//-------------------------------------------------------------------------------------------------------	
//Graba el archivo en el Gabinete de Archivos
//-------------------------------------------------------------------------------------------------------
function savefile(pNombreFile, pTipoArchivo){
	// Ruta de la carpeta contenedora
	var FolderId = nlapiGetContext().getSetting('SCRIPT', 'custscript_lmry_file_cabinet_rg_co');
	
	// Almacena en la carpeta de Archivos Generados
	if (FolderId!='' && FolderId!=null) {
		// Genera el nombre del archivo
		var NameFile = pNombreFile; 
		
		// Crea el archivo
		var File = nlapiCreateFile(NameFile, pTipoArchivo, strName);	
			File.setFolder(FolderId);
	
		// Termina de grabar el archivo
		var idfile = nlapiSubmitFile(File);
	
		// Trae URL de archivo generado
		var idfile2 = nlapiLoadFile(idfile);
	
		// Obtenemo de las prefencias generales el URL de Netsuite (Produccion o Sandbox)
		var getURL = objContext.getSetting('SCRIPT', 'custscript_lmry_netsuite_location');
		var urlfile = '';
			if (getURL!='' && getURL!=''){
				urlfile += 'https://' + getURL;
			}
			urlfile += idfile2.getURL();
		
		//Genera registro personalizado como log
		if(idfile) {
			var usuario = objContext.getName();
			
 		    // Se graba el en log de archivos generados del reporteador
			var record = nlapiLoadRecord(RecordName, paramidrpt);	// generator_log
				record.setFieldValue(RecordTable[0], NameFile);		// name
				record.setFieldValue(RecordTable[1], periodname);	// postingperiod
				record.setFieldValue(RecordTable[2], companyname);		// subsidiary
				record.setFieldValue(RecordTable[3], urlfile);		// url_file
				record.setFieldValue(RecordTable[4], usuario);		// employee
			nlapiSubmitRecord(record, true); 
			
			sendrptuser(NameFile);
			
		}
	} else {
		// Debug
		nlapiLogExecution('ERROR', 'Creacion de EXCEL', 'No se existe el folder');
	}
}
//-------------------------------------------------------------------------------------------------------	
//Obtiene nombre de Subsidiaria
//-------------------------------------------------------------------------------------------------------
function ObtainNameSubsidiaria(subsidiari)
{
	try{
		if (subsidiari != '' && subsidiari != null) {
			var Name = nlapiLookupField('subsidiary', subsidiari, 'legalname');
			return Name;			 
		}
	}catch(err){
		sendemail(' [ ObtainNameSubsidiaria ] ' +err, LMRY_script);
	}
	return '';
}

//-------------------------------------------------------------------------------------------------------	
//Obtiene el n?mero de identificaci?n fiscal de la subsidiaria
//-------------------------------------------------------------------------------------------------------
function ObtainFederalIdSubsidiaria(subsidiari)
{
	try{
		if (subsidiari != '' && subsidiari != null) {
			var FederalIdNumber = nlapiLookupField('subsidiary', subsidiari, 'taxidnum');
			return FederalIdNumber;			 
		}
	}catch(err){
		sendemail(' [ ObtainFederalIdSubsidiaria ] ' +err, LMRY_script);
	}
	return '';
}
//-------------------------------------------------------------------------------------------------------	
//Obtiene a?o y mes del periodo
//-------------------------------------------------------------------------------------------------------
function Periodo(periodo) {
	var auxfech = '';
	
	auxanio= periodo.substring(4);
	switch (periodo.substring(0, 3).toLowerCase()) {
	  case 'ene', 'jan':
		  auxmess = '01';
		  break;
	  case 'feb':
		  auxmess = '02';
		  break;
	  case 'mar':
		  auxmess = '03';
		  break;
	  case 'abr', 'apr':
		  auxmess = '04';
		  break;
	  case 'may':
		  auxmess = '05';
		  break;
	  case 'jun':
		  auxmess = '06';
		  break;
	  case 'jul':
		  auxmess = '07';
		  break;
	  case 'ago', 'aug':
		  auxmess = '08';
		  break;
	  case 'set', 'sep':
		  auxmess = '09';
		  break;
	  case 'oct':
		  auxmess = '10';
		  break;
	  case 'nov':
		  auxmess = '11';
		  break;
	  case 'dic', 'dec':
		  auxmess = '12';
		  break;
	  default:
		  auxmess = '00';
		  break;
	}
	auxfech = auxanio + auxmess + '00';
	return;
}