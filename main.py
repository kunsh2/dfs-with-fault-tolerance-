import sys
import os
import socket
import threading
import pickle
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QListWidget, QMessageBox, QFileDialog, QDialog, QDialogButtonBox
)
from PyQt5.QtCore import Qt, QTimer

# --------------------
# Configuration
# --------------------
NODE_PORTS = [5001, 5002, 5003]  # 3 nodes
UPDATE_INTERVAL_MS = 1000       # 1 second updates
DOWNLOADS_DIR = "downloads"


# --------------------
# Node (server) class
# --------------------
class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.port = NODE_PORTS[node_id - 1]
        self.folder = f"files/node{node_id}"
        os.makedirs(self.folder, exist_ok=True)

        self.files = {}
        for fn in os.listdir(self.folder):
            fp = os.path.join(self.folder, fn)
            if os.path.isfile(fp):
                try:
                    with open(fp, "rb") as f:
                        self.files[fn] = f.read()
                except Exception:
                    pass

        self.alive = False
        self._sock = None
        self._thread = None

    def start(self):
        if self.alive:
            return
        self.alive = True
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def stop(self):
        self.alive = False
        try:
            if self._sock:
                try:
                    self._sock.shutdown(socket.SHUT_RDWR)
                except:
                    pass
                self._sock.close()
        except:
            pass
        self._sock = None

    def _serve(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("localhost", self.port))
            s.listen(5)
            s.settimeout(1.0)
            self._sock = s
            while self.alive:
                try:
                    conn, _ = s.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break

                threading.Thread(target=self._handle_conn, args=(conn,), daemon=True).start()
        except Exception:
            pass
        finally:
            try:
                s.close()
            except:
                pass
            self._sock = None
            self.alive = False

    def _handle_conn(self, conn):
        try:
            data = conn.recv(10 * 1024 * 1024)
            if not data:
                try: conn.close()
                except: pass
                return
            try:
                req = pickle.loads(data)
            except Exception:
                try: conn.close()
                except: pass
                return

            action = req.get("action")
            if action == "upload":
                name = req.get("filename")
                content = req.get("content", b"")
                if name:
                    try:
                        self.files[name] = content
                        with open(os.path.join(self.folder, name), "wb") as f:
                            f.write(content)
                        conn.send(b"OK")
                    except Exception:
                        conn.send(b"ERR")
                else:
                    conn.send(b"ERR")

            elif action == "list":
                try:
                    conn.send(pickle.dumps(list(self.files.keys())))
                except Exception:
                    conn.send(pickle.dumps([]))

            elif action == "download":
                name = req.get("filename")
                if name in self.files:
                    conn.send(self.files[name])
                else:
                    conn.send(b"")  # empty --> not found
            else:
                conn.send(b"ERR")
        finally:
            try:
                conn.close()
            except:
                pass


# --------------------
# Client helper funcs
# --------------------
def send_cmd(node_id, cmd, timeout=1.0):
    port = NODE_PORTS[node_id - 1]
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect(("localhost", port))
        s.send(pickle.dumps(cmd))
        data = s.recv(10 * 1024 * 1024)
        s.close()
        return data
    except Exception:
        try:
            s.close()
        except:
            pass
        return None


def upload_to_node(node_id, filename, content):
    res = send_cmd(node_id, {"action": "upload", "filename": filename, "content": content})
    return res is not None


def upload_to_all_nodes(filename, content):
    results = {}
    for i in range(1, len(NODE_PORTS) + 1):
        ok = upload_to_node(i, filename, content)
        results[i] = ok
    return results


def list_files_on_node(node_id):
    data = send_cmd(node_id, {"action": "list"})
    if data is None:
        return None
    try:
        return pickle.loads(data)
    except Exception:
        return []


def download_from_node(node_id, filename):
    data = send_cmd(node_id, {"action": "download", "filename": filename})
    if data is None or data == b"":
        return None
    return data


def download_from_any_node(filename):
    for i in range(1, len(NODE_PORTS) + 1):
        content = download_from_node(i, filename)
        if content is not None:
            return content, i
    return None, None


# --------------------
# GUI: Per-node file dialog
# --------------------
class NodeFilesDialog(QDialog):
    def __init__(self, parent, node_idx):
        super().__init__(parent)
        self.setWindowTitle(f"Node {node_idx+1} Files")
        self.setMinimumWidth(400)
        self.node_idx = node_idx
        layout = QVBoxLayout()
        self.files_list = QListWidget()
        layout.addWidget(self.files_list)

        btns = QHBoxLayout()
        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.clicked.connect(self.refresh)
        btns.addWidget(self.refresh_btn)

        self.download_btn = QPushButton("Download Selected from this node")
        self.download_btn.clicked.connect(self.download_selected)
        btns.addWidget(self.download_btn)

        layout.addLayout(btns)

        box = QDialogButtonBox(QDialogButtonBox.Close)
        box.rejected.connect(self.reject)
        layout.addWidget(box)

        self.setLayout(layout)
        self.refresh()

    def refresh(self):
        self.files_list.clear()
        files = list_files_on_node(self.node_idx + 1)
        if files is None:
            self.files_list.addItem("(OFFLINE)")
            self.download_btn.setEnabled(False)
            return
        if not files:
            self.files_list.addItem("(no files)")
        else:
            for fn in sorted(files):
                self.files_list.addItem(fn)
        self.download_btn.setEnabled(True)

    def download_selected(self):
        item = self.files_list.currentItem()
        if not item:
            QMessageBox.information(self, "Download", "Select a file first.")
            return
        name = item.text()
        if name in ("(OFFLINE)", "(no files)"):
            QMessageBox.warning(self, "Download", "No valid file selected.")
            return
        content = download_from_node(self.node_idx + 1, name)
        if content is None:
            QMessageBox.warning(self, "Download", "File not available on that node or node offline.")
            return
        # Save to downloads folder by default
        os.makedirs(DOWNLOADS_DIR, exist_ok=True)
        path = os.path.join(DOWNLOADS_DIR, name)
        try:
            with open(path, "wb") as f:
                f.write(content)
        except Exception as e:
            QMessageBox.warning(self, "Download", f"Failed to save file: {e}")
            return
        QMessageBox.information(self, "Download", f"Saved to {path} (from Node {self.node_idx+1})")


# --------------------
# Main GUI
# --------------------
class DFSMain(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Single-file DFS (replication) - GUI")
        self.setMinimumSize(800, 600)
        self._ensure_dirs()

        layout = QVBoxLayout()

        # Node status horizontal row
        status_row = QHBoxLayout()
        self.node_labels = []
        for i in range(len(NODE_PORTS)):
            lbl = QLabel()
            lbl.setAlignment(Qt.AlignCenter)
            lbl.setFixedWidth(240)
            lbl.setStyleSheet("font-weight:bold;")
            status_row.addWidget(lbl)
            self.node_labels.append(lbl)
        layout.addLayout(status_row)

        # Buttons row
        btn_row = QHBoxLayout()
        self.upload_btn = QPushButton("Upload File (replicate to all)")
        self.upload_btn.clicked.connect(self.upload_file)
        btn_row.addWidget(self.upload_btn)

        self.download_btn = QPushButton("Download Selected (from any node)")
        self.download_btn.clicked.connect(self.download_selected)
        btn_row.addWidget(self.download_btn)

        # Per-node view buttons
        for i in range(len(NODE_PORTS)):
            view_btn = QPushButton(f"View Node {i+1} Files")
            view_btn.clicked.connect(lambda _, x=i: self.open_node_dialog(x))
            btn_row.addWidget(view_btn)

        layout.addLayout(btn_row)

        # All-files list (panel that always shows current all-files across nodes)
        self.files_list = QListWidget()
        layout.addWidget(self.files_list)

        # Stop/Start row
        ctrl_row = QHBoxLayout()
        for i in range(len(NODE_PORTS)):
            stop_b = QPushButton(f"Stop Node {i+1}")
            stop_b.clicked.connect(lambda _, x=i: self.stop_node(x))
            ctrl_row.addWidget(stop_b)
            start_b = QPushButton(f"Start Node {i+1}")
            start_b.clicked.connect(lambda _, x=i: self.start_node(x))
            ctrl_row.addWidget(start_b)
        layout.addLayout(ctrl_row)

        self.setLayout(layout)

        # Timer for updates every 1 second
        self.timer = QTimer()
        self.timer.setInterval(UPDATE_INTERVAL_MS)
        self.timer.timeout.connect(self.refresh)
        self.timer.start()
        self.refresh()

    def _ensure_dirs(self):
        os.makedirs("files", exist_ok=True)
        for i in range(1, len(NODE_PORTS) + 1):
            os.makedirs(f"files/node{i}", exist_ok=True)
        os.makedirs(DOWNLOADS_DIR, exist_ok=True)

    def refresh(self):
        # update node labels and gather all files
        all_files = set()
        for i in range(len(NODE_PORTS)):
            files = list_files_on_node(i + 1)
            alive = files is not None
            if alive:
                count = len(files)
                for f in files:
                    all_files.add(f)
                self.node_labels[i].setText(f"Node {i+1}\nACTIVE\nFiles: {count}")
                self.node_labels[i].setStyleSheet("background: green; color: white; padding:8px; border-radius:6px;")
            else:
                self.node_labels[i].setText(f"Node {i+1}\nOFFLINE")
                self.node_labels[i].setStyleSheet("background: red; color: white; padding:8px; border-radius:6px;")

        # update all-files panel
        self.files_list.clear()
        for fn in sorted(all_files):
            self.files_list.addItem(fn)

    def upload_file(self):
        fname, _ = QFileDialog.getOpenFileName(self, "Select File to Upload (replicated to all nodes)")
        if not fname:
            return
        try:
            with open(fname, "rb") as f:
                content = f.read()
        except Exception as e:
            QMessageBox.warning(self, "Upload", f"Could not read file: {e}")
            return

        filename = os.path.basename(fname)
        results = upload_to_all_nodes(filename, content)
        # Build message showing per-node success/failure
        ok_nodes = [str(n) for n, ok in results.items() if ok]
        failed_nodes = [str(n) for n, ok in results.items() if not ok]
        msg = f"Upload results:\nSucceeded: {', '.join(ok_nodes) if ok_nodes else 'None'}\nFailed: {', '.join(failed_nodes) if failed_nodes else 'None'}"
        QMessageBox.information(self, "Upload", msg)
        self.refresh()

    def download_selected(self):
        item = self.files_list.currentItem()
        if not item:
            QMessageBox.information(self, "Download", "Select a file from the All Files list first.")
            return
        name = item.text()
        content, from_node = download_from_any_node(name)
        if content is None:
            QMessageBox.warning(self, "Download", "File not available on any active node.")
            return
        # Save by default to downloads/
        path = os.path.join(DOWNLOADS_DIR, name)
        try:
            with open(path, "wb") as f:
                f.write(content)
        except Exception as e:
            QMessageBox.warning(self, "Download", f"Failed to save file: {e}")
            return
        QMessageBox.information(self, "Download", f"Saved to {path} (from Node {from_node})")

    def open_node_dialog(self, node_idx):
        dlg = NodeFilesDialog(self, node_idx)
        dlg.exec_()
        # after dialog closes, refresh main list
        self.refresh()

    def stop_node(self, idx):
        try:
            nodes[idx].stop()
        except Exception:
            pass
        self.refresh()

    def start_node(self, idx):
        try:
            nodes[idx].start()
        except Exception:
            pass
        self.refresh()


# --------------------
# Launch nodes & app
# --------------------
nodes = []


def launch_nodes():
    global nodes
    nodes.clear()
    for i in range(1, len(NODE_PORTS) + 1):
        n = Node(i)
        nodes.append(n)
        n.start()


if __name__ == "__main__":
    launch_nodes()
    app = QApplication(sys.argv)
    w = DFSMain()
    w.show()
    sys.exit(app.exec_())
