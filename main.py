import sys
import os
import socket
import threading
import pickle
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QListWidget, QMessageBox, QFileDialog, QDialog, QDialogButtonBox, QComboBox
)
from PyQt5.QtCore import Qt, QTimer

NODE_PORTS = [5001, 5002, 5003]
UPDATE_INTERVAL_MS = 1000
DOWNLOADS_DIR = "downloads"

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
                except:
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
        except:
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
            except:
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
                    except:
                        conn.send(b"ERR")
                else:
                    conn.send(b"ERR")
            elif action == "list":
                try:
                    conn.send(pickle.dumps(list(self.files.keys())))
                except:
                    conn.send(pickle.dumps([]))
            elif action == "download":
                name = req.get("filename")
                if name in self.files:
                    conn.send(self.files[name])
                else:
                    conn.send(b"")
            elif action == "delete":
                name = req.get("filename")
                if name in self.files:
                    try:
                        del self.files[name]
                        fp = os.path.join(self.folder, name)
                        if os.path.exists(fp):
                            os.remove(fp)
                        conn.send(b"OK")
                    except:
                        conn.send(b"ERR")
                else:
                    conn.send(b"ERR")
            else:
                conn.send(b"ERR")
        finally:
            try:
                conn.close()
            except:
                pass

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
    except:
        try: s.close()
        except: pass
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
    except:
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

def delete_from_node(node_id, filename):
    data = send_cmd(node_id, {"action": "delete", "filename": filename})
    return data == b"OK"

def delete_from_all_nodes(filename):
    results = {}
    for i in range(1, len(NODE_PORTS) + 1):
        ok = delete_from_node(i, filename)
        results[i] = ok
    return results

class SpecificDeleteDialog(QDialog):
    def __init__(self, parent):
        super().__init__(parent)
        self.setWindowTitle("Delete from a specific node")
        self.setMinimumWidth(300)
        layout = QVBoxLayout()
        self.file_box = QComboBox()
        for i in range(parent.files_list.count()):
            self.file_box.addItem(parent.files_list.item(i).text())
        layout.addWidget(self.file_box)
        self.node_box = QComboBox()
        for i in range(len(NODE_PORTS)):
            self.node_box.addItem(f"Node {i+1}", i+1)
        layout.addWidget(self.node_box)
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)
        self.setLayout(layout)

class NodeFilesDialog(QDialog):
    def __init__(self, parent, node_idx):
        super().__init__(parent)
        self.setWindowTitle(f"Node {node_idx+1} Files")
        self.setMinimumWidth(400)
        self.node_idx = node_idx
        layout = QVBoxLayout()
        self.files_list = QListWidget()
        self.files_list.setSelectionMode(QListWidget.SingleSelection)
        layout.addWidget(self.files_list)
        btns = QHBoxLayout()
        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.clicked.connect(self.refresh)
        btns.addWidget(self.refresh_btn)
        self.download_btn = QPushButton("Download Selected")
        self.download_btn.clicked.connect(self.download_selected)
        btns.addWidget(self.download_btn)
        layout.addLayout(btns)
        box = QDialogButtonBox(QDialogButtonBox.Close)
        box.rejected.connect(self.reject)
        layout.addWidget(box)
        self.setLayout(layout)
        self.refresh()

    def refresh(self):
        n = self.files_list.currentItem().text() if self.files_list.currentItem() else None
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
        if n:
            items = self.files_list.findItems(n, Qt.MatchExactly)
            if items:
                self.files_list.setCurrentItem(items[0])

    def download_selected(self):
        item = self.files_list.currentItem()
        if not item:
            QMessageBox.information(self, "Download", "Select a file first.")
            return
        name = item.text()
        if name in ("(OFFLINE)", "(no files)"):
            return
        content = download_from_node(self.node_idx + 1, name)
        if content is None:
            QMessageBox.warning(self, "Download", "Unavailable.")
            return
        os.makedirs(DOWNLOADS_DIR, exist_ok=True)
        path = os.path.join(DOWNLOADS_DIR, name)
        try:
            with open(path, "wb") as f:
                f.write(content)
        except Exception as e:
            QMessageBox.warning(self, "Download", str(e))
            return
        QMessageBox.information(self, "Download", f"Saved to {path}")

class DFSMain(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Single-file DFS (replication)")
        self.setMinimumSize(800, 600)
        self._ensure_dirs()
        layout = QVBoxLayout()
        status_row = QHBoxLayout()
        self.node_labels = []
        for i in range(len(NODE_PORTS)):
            lbl = QLabel()
            lbl.setAlignment(Qt.AlignCenter)
            lbl.setFixedWidth(240)
            status_row.addWidget(lbl)
            self.node_labels.append(lbl)
        layout.addLayout(status_row)
        btn_row = QHBoxLayout()
        self.upload_btn = QPushButton("Upload File (replicate to all)")
        self.upload_btn.clicked.connect(self.upload_file)
        btn_row.addWidget(self.upload_btn)
        self.download_btn = QPushButton("Download Selected")
        self.download_btn.clicked.connect(self.download_selected)
        btn_row.addWidget(self.download_btn)
        self.delete_btn = QPushButton("Delete from all nodes")
        self.delete_btn.clicked.connect(self.delete_selected)
        btn_row.addWidget(self.delete_btn)
        self.delete_specific_btn = QPushButton("Delete from specific node")
        self.delete_specific_btn.clicked.connect(self.delete_specific)
        btn_row.addWidget(self.delete_specific_btn)
        for i in range(len(NODE_PORTS)):
            view_btn = QPushButton(f"View Node {i+1} Files")
            view_btn.clicked.connect(lambda _, x=i: self.open_node_dialog(x))
            btn_row.addWidget(view_btn)
        layout.addLayout(btn_row)
        self.files_list = QListWidget()
        self.files_list.setSelectionMode(QListWidget.SingleSelection)
        layout.addWidget(self.files_list)
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
        old = self.files_list.currentItem().text() if self.files_list.currentItem() else None
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
        self.files_list.clear()
        for fn in sorted(all_files):
            self.files_list.addItem(fn)
        if old:
            items = self.files_list.findItems(old, Qt.MatchExactly)
            if items:
                self.files_list.setCurrentItem(items[0])

    def upload_file(self):
        fname, _ = QFileDialog.getOpenFileName(self, "Select File")
        if not fname:
            return
        try:
            with open(fname, "rb") as f:
                content = f.read()
        except Exception as e:
            QMessageBox.warning(self, "Upload", str(e))
            return
        filename = os.path.basename(fname)
        results = upload_to_all_nodes(filename, content)
        ok_nodes = [str(n) for n, ok in results.items() if ok]
        failed_nodes = [str(n) for n, ok in results.items() if not ok]
        msg = f"Succeeded: {', '.join(ok_nodes) if ok_nodes else 'None'}\nFailed: {', '.join(failed_nodes) if failed_nodes else 'None'}"
        QMessageBox.information(self, "Upload", msg)
        self.refresh()

    def download_selected(self):
        item = self.files_list.currentItem()
        if not item:
            QMessageBox.information(self, "Download", "Select a file.")
            return
        name = item.text()
        content, from_node = download_from_any_node(name)
        if content is None:
            QMessageBox.warning(self, "Download", "Unavailable.")
            return
        path = os.path.join(DOWNLOADS_DIR, name)
        try:
            with open(path, "wb") as f:
                f.write(content)
        except Exception as e:
            QMessageBox.warning(self, "Download", str(e))
            return
        QMessageBox.information(self, "Download", f"Saved to {path} (Node {from_node})")

    def delete_selected(self):
        item = self.files_list.currentItem()
        if not item:
            QMessageBox.information(self, "Delete", "Select a file.")
            return
        name = item.text()
        reply = QMessageBox.question(self, "Confirm", f"Delete '{name}' from ALL nodes?", QMessageBox.Yes | QMessageBox.No)
        if reply != QMessageBox.Yes:
            return
        results = delete_from_all_nodes(name)
        ok_nodes = [str(n) for n, ok in results.items() if ok]
        failed_nodes = [str(n) for n, ok in results.items() if not ok]
        msg = f"Succeeded: {', '.join(ok_nodes) if ok_nodes else 'None'}\nFailed: {', '.join(failed_nodes) if failed_nodes else 'None'}"
        QMessageBox.information(self, "Delete", msg)
        self.refresh()

    def delete_specific(self):
        dlg = SpecificDeleteDialog(self)
        if dlg.exec_() != QDialog.Accepted:
            return
        filename = dlg.file_box.currentText()
        node_id = dlg.node_box.currentData()
        ok = delete_from_node(node_id, filename)
        if ok:
            QMessageBox.information(self, "Delete", f"Deleted from Node {node_id}")
        else:
            QMessageBox.warning(self, "Delete", f"Failed on Node {node_id}")
        self.refresh()

    def open_node_dialog(self, node_idx):
        dlg = NodeFilesDialog(self, node_idx)
        dlg.exec_()
        self.refresh()

    def stop_node(self, idx):
        try:
            nodes[idx].stop()
        except:
            pass
        self.refresh()

    def start_node(self, idx):
        try:
            nodes[idx].start()
        except:
            pass
        self.refresh()

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
