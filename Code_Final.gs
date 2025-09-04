// =============================
// CONFIGURATION - UPDATE THESE VALUES
// =============================
const SHEET_ID   = 'AKfycbz4dh4zrLrKch3IAYk-SdZa4WH9THb90MbtOvjeC3_Melbvtf8oT7774O42khA588JWmQ'; // Optional: Spreadsheet ID. If empty, uses active spreadsheet.
const SHEET_NAME = 'Routing';                   // 👈 UPDATE if your tab has different name


// Expected headers in row 1
const EXPECTED_HEADERS = [
  'PartNumber',
  'OperationSeq',
  'OperationName',
  'SetupTime_Min',
  'Operator',
  'CycleTime_Min',
  'Minimum_BatchSize',
  'EligibleMachines'
];


// =============================
// HELPERS
// =============================
function _getSheet_() {
  const ss = SHEET_ID && SHEET_ID.trim()
    ? SpreadsheetApp.openById(SHEET_ID)
    : SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(SHEET_NAME);
  if (!sh) throw new Error(`Sheet "${SHEET_NAME}" not found`);
  return sh;
}


function _validateHeaders_(values) {
  if (!values || values.length === 0) throw new Error('Sheet is empty');
  const headers = values[0].map(String);
  if (headers.length !== EXPECTED_HEADERS.length) {
    return { ok: false, reason: `Expected ${EXPECTED_HEADERS.length} headers, got ${headers.length}` };
  }
  for (let i = 0; i < EXPECTED_HEADERS.length; i++) {
    if (headers[i] !== EXPECTED_HEADERS[i]) {
      return { ok: false, reason: `Column ${i+1} should be "${EXPECTED_HEADERS[i]}", got "${headers[i]}"` };
    }
  }
  return { ok: true };
}


function _rowsToObjects_(rows) {
  return rows.map(r => {
    return {
      PartNumber:        r[0],
      OperationSeq:      Number(r[1]),
      OperationName:     r[2],
      SetupTime_Min:     Number(r[3]),
      Operator:          r[4],
      CycleTime_Min:     Number(r[5]),
      Minimum_BatchSize: Number(r[6]),
      EligibleMachines:  r[7]
    };
  });
}


function _jsonResponse_(body) {
  return ContentService
    .createTextOutput(JSON.stringify(body))
    .setMimeType(ContentService.MimeType.JSON);
}

function _jsonpResponse_(callbackName, body) {
  const payload = `${callbackName}(${JSON.stringify(body)});`;
  return ContentService
    .createTextOutput(payload)
    .setMimeType(ContentService.MimeType.JAVASCRIPT);
}


// =============================
// GET HANDLER - Read Operations
// =============================
function doGet(e) {
  try {
    const sh = _getSheet_();
    const values = sh.getDataRange().getValues();

    const check = _validateHeaders_(values);
    if (!check.ok) {
      return _jsonResponse_({ error: 'Invalid column structure', details: check.reason });
    }

    const rows = values.slice(1).filter(r => r.some(c => String(c).trim() !== ''));
    const items = _rowsToObjects_(rows);

    const body = {
      meta: {
        sheetId: SHEET_ID,
        sheetName: SHEET_NAME,
        lastSync: new Date().toISOString(),
        rowCount: items.length
      },
      items
    };
    if (e && e.parameter && e.parameter.callback) {
      return _jsonpResponse_(e.parameter.callback, body);
    }
    return _jsonResponse_(body);
  } catch (err) {
    const body = { error: String(err) };
    if (e && e.parameter && e.parameter.callback) {
      return _jsonpResponse_(e.parameter.callback, body);
    }
    return _jsonResponse_(body);
  }
}


// =============================
// POST HANDLER - Write/Update Operations
// =============================
function doPost(e) {
  try {
    const postData = JSON.parse(e.postData.contents);
    const action = postData.action;
    
    switch (action) {
      case 'addRow':
        return _addRow_(postData.data);
      case 'updateRow':
        return _updateRow_(postData.rowIndex, postData.data);
      case 'deleteRow':
        return _deleteRow_(postData.rowIndex);
      default:
        throw new Error(`Unknown action: ${action}`);
    }
  } catch (err) {
    return _jsonResponse_({ error: String(err) });
  }
}

function _addRow_(rowData) {
  const sh = _getSheet_();
  const newRow = [
    rowData.PartNumber || '',
    rowData.OperationSeq || 0,
    rowData.OperationName || '',
    rowData.SetupTime_Min || 0,
    rowData.Operator || '',
    rowData.CycleTime_Min || 0,
    rowData.Minimum_BatchSize || 1,
    rowData.EligibleMachines || ''
  ];
  
  sh.appendRow(newRow);
  
  return _jsonResponse_({
    success: true,
    message: 'Row added successfully',
    data: rowData
  });
}

function _updateRow_(rowIndex, rowData) {
  const sh = _getSheet_();
  const actualRow = rowIndex + 2; // +1 for header, +1 for 0-based index
  
  const updatedRow = [
    rowData.PartNumber || '',
    rowData.OperationSeq || 0,
    rowData.OperationName || '',
    rowData.SetupTime_Min || 0,
    rowData.Operator || '',
    rowData.CycleTime_Min || 0,
    rowData.Minimum_BatchSize || 1,
    rowData.EligibleMachines || ''
  ];
  
  const range = sh.getRange(actualRow, 1, 1, EXPECTED_HEADERS.length);
  range.setValues([updatedRow]);
  
  return _jsonResponse_({
    success: true,
    message: 'Row updated successfully',
    rowIndex: rowIndex,
    data: rowData
  });
}

function _deleteRow_(rowIndex) {
  const sh = _getSheet_();
  const actualRow = rowIndex + 2; // +1 for header, +1 for 0-based index
  
  sh.deleteRow(actualRow);
  
  return _jsonResponse_({
    success: true,
    message: 'Row deleted successfully',
    rowIndex: rowIndex
  });
}

// =============================
// CORS preflight (for fetch)
// =============================
function doOptions(e) {
  return _jsonResponse_({});
}
