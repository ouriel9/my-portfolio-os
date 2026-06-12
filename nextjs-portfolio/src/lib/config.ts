/**
 * Server-only environment loader. Must NEVER be imported from a client
 * component or any code that runs in the browser.
 */
export interface AppsScriptConfig {
  url: string;
  token: string;
  spreadsheetRef: string;
  worksheetName: string;
}

export function getAppsScriptConfig(): AppsScriptConfig {
  const url = process.env.APPS_SCRIPT_URL;
  const token = process.env.APPS_SCRIPT_TOKEN;
  if (!url || !token) {
    throw new Error(
      'APPS_SCRIPT_URL and APPS_SCRIPT_TOKEN must be set in .env.local — see README.',
    );
  }
  return {
    url,
    token,
    spreadsheetRef: process.env.SPREADSHEET_REF ?? '',
    worksheetName: process.env.WORKSHEET_NAME ?? 'תמונת מצב',
  };
}
