function doGet(e) {
  // get the active sheet (no need for ID!)
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Routing");
  const data = sheet.getDataRange().getValues();
  const headers = data.shift();

  // convert rows to JSON
  const jsonData = data.map(row => {
    let obj = {};
    headers.forEach((h, i) => obj[h] = row[i]);
    return obj;
  });

  return ContentService.createTextOutput(
    JSON.stringify({
      meta: {
        sheetName: "Routing",
        lastSync: new Date().toISOString(),
        rowCount: jsonData.length
      },
      items: jsonData
    })
  ).setMimeType(ContentService.MimeType.JSON)
   .setHeader('Access-Control-Allow-Origin', '*')
   .setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
   .setHeader('Access-Control-Allow-Headers', 'Content-Type');
}
