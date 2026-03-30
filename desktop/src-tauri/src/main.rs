#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::{
    fs,
    net::TcpStream,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

use tauri::{Manager, State, WindowEvent};

type SharedChild = Arc<Mutex<Option<Child>>>;

struct BackendState {
    child: SharedChild,
}

#[tauri::command]
fn backend_status() -> bool {
    TcpStream::connect_timeout(&"127.0.0.1:8088".parse().unwrap(), Duration::from_millis(350)).is_ok()
}

#[tauri::command]
fn backend_logs(state: State<BackendState>) -> Result<String, String> {
    let child_lock = state.child.lock().map_err(|_| "backend lock poisoned")?;
    if child_lock.is_some() {
        Ok("Backend process is running under Tauri supervision.".to_string())
    } else {
        Ok("Backend process is not running.".to_string())
    }
}

fn candidate_backend_paths(app_dir: &PathBuf) -> Vec<PathBuf> {
    vec![
        app_dir.join("..").join("..").join("backend").join("bin").join("aqe-backend-x86_64-pc-windows-gnu.exe"),
        app_dir.join("..").join("..").join("backend").join("bin").join("aqe-backend.exe"),
        app_dir.join("aqe-backend-x86_64-pc-windows-gnu.exe"),
        app_dir.join("aqe-backend.exe"),
        app_dir.join("aqe-backend"),
    ]
}

fn spawn_backend(app_dir: &PathBuf) -> Option<Child> {
    let env_override = std::env::var("AQE_BACKEND_BIN").ok().map(PathBuf::from);
    let mut candidates = env_override.into_iter().collect::<Vec<_>>();
    candidates.extend(candidate_backend_paths(app_dir));

    for candidate in candidates {
        if !candidate.exists() {
            continue;
        }

        let mut command = Command::new(&candidate);
        command
            .env("AQE_PORT", "8088")
            .env("AQE_DUCKDB_PATH", app_dir.join("aqe.duckdb").to_string_lossy().to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        if let Ok(child) = command.spawn() {
            return Some(child);
        }
    }
    None
}

fn shutdown_child(child_handle: &SharedChild) {
    let mut guard = match child_handle.lock() {
        Ok(guard) => guard,
        Err(_) => return,
    };

    if let Some(child) = guard.as_mut() {
        let _ = child.kill();
    }
    *guard = None;
}

fn main() {
    tauri::Builder::default()
        .manage(BackendState {
            child: Arc::new(Mutex::new(None)),
        })
        .setup(|app| {
            let app_dir = app
                .path()
                .app_data_dir()
                .map_err(|err| format!("failed to resolve app data dir: {err}"))?;
            fs::create_dir_all(&app_dir)
                .map_err(|err| format!("failed to create app data dir: {err}"))?;

            let child = spawn_backend(&app_dir);
            let state = app.state::<BackendState>();
            let mut guard = state
                .child
                .lock()
                .map_err(|_| "backend lock poisoned".to_string())?;
            *guard = child;
            Ok(())
        })
        .on_window_event(|window, event| {
            if matches!(event, WindowEvent::Destroyed) {
                let child_handle = {
                    let state = window.state::<BackendState>();
                    Arc::clone(&state.child)
                };
                shutdown_child(&child_handle);
            }
        })
        .invoke_handler(tauri::generate_handler![backend_status, backend_logs])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
