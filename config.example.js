// Configuration for GitHub Pages deployment
// Copy this to config.js and update with your values

window.CONFIG = {
  // Supabase Configuration
  SUPABASE_URL: 'https://sxnaopzgaddvziplrlbe.supabase.co',
  SUPABASE_ANON_KEY: 'your-supabase-anon-key-here',
  
  // Google Sheets API Configuration
  GOOGLE_SHEETS_API_URL: 'https://script.google.com/macros/s/your-script-id/exec',
  
  // Application Settings
  AUTO_SYNC_INTERVAL_MINUTES: 5,
  MAX_CONCURRENT_SETUPS: 2,
  DEFAULT_SETUP_START_HOUR: 6,
  DEFAULT_SETUP_END_HOUR: 22,
  
  // Feature Flags
  ENABLE_GOOGLE_SHEETS_SYNC: true,
  ENABLE_LOCAL_DATA_FALLBACK: true,
  ENABLE_AUTO_SYNC: true
};
