import { createClient } from '@supabase/supabase-js'

const supabaseUrl = 'https://eltwiuopscmtpyxlurte.supabase.co'
const supabaseAnonKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVsdHdpdW9wc2NtdHB5eGx1cnRlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQwMzA5MzEsImV4cCI6MjA4OTYwNjkzMX0.115t2c5Qb0TfKW3l8PQY-1lwM3Nrk31D1aFwavs4i1c'

export const supabase = createClient(supabaseUrl, supabaseAnonKey)
