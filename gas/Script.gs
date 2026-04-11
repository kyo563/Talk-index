/**
 * 索引シートの重複行を削除します。
 * 条件: A〜H列の値がすべて一致する行を重複とみなし、先頭1件のみ残します。
 *
 * 使い方:
 * 1) Google Apps Script に貼り付け
 * 2) removeDuplicateRowsByColumnsAtoH() を実行
 */
function removeDuplicateRowsByColumnsAtoH() {
  var sheetName = '索引';
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);

  if (!sheet) {
    throw new Error('シート「' + sheetName + '」が見つかりません。シート名を確認してください。');
  }

  var lastRow = sheet.getLastRow();
  if (lastRow <= 1) {
    Logger.log('データ行がないため、削除対象はありません。');
    return;
  }

  // 2行目以降のA〜H列を取得
  var numRows = lastRow - 1;
  var values = sheet.getRange(2, 1, numRows, 8).getValues();

  var seen = {};
  var rowsToDelete = [];

  for (var i = 0; i < values.length; i++) {
    var row = values[i];
    var key = row.map(function (cell) {
      return String(cell).trim();
    }).join('||');

    if (seen[key]) {
      // シート上の実行行番号に変換（ヘッダー分 +1）
      rowsToDelete.push(i + 2);
    } else {
      seen[key] = true;
    }
  }

  // 下から削除して行ズレを防ぐ
  for (var j = rowsToDelete.length - 1; j >= 0; j--) {
    sheet.deleteRow(rowsToDelete[j]);
  }

  Logger.log('重複削除が完了しました。削除行数: ' + rowsToDelete.length);
}
