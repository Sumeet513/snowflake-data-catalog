@echo off
echo Starting the full project (Backend + Frontend)...

REM Start the Django backend server in a new terminal
start cmd /k "echo Starting Django backend server... && cd backend && python manage.py runserver"

REM Wait a moment for the backend to initialize
timeout /t 5

REM Start the React frontend server in a new terminal
start cmd /k "echo Starting React frontend... && cd frontend && npm start"

echo Both servers have been started.
echo Backend is running at: http://localhost:8000
echo Frontend is running at: http://localhost:3000