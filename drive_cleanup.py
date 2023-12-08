# coding=utf-8
# authors: Outcast Inc
# created: 12/08/2023

from __future__ import division
from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
import re
import os
import sys
import math
import psutil
import scandir
import threading
import subprocess
import multiprocessing
from PySide2 import QtWidgets
from PySide2 import QtCore
from PySide2 import QtGui

import logging
logging.basicConfig(filename='DriveCleanup.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

__TOOL_NAME__ = "Drive Cleanup Tool"
__VERSION__ = "1.0.0"
__BASE_PATHS__ = [r""]  # Add paths to scan
LOCAL_STORAGE_DRIVE = ""  # Set Local Storage Drive


def convert_size(_bytes):
    if _bytes == 0:
        return "0B", 0
    size_str = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    log = int(math.floor(math.log(_bytes, 1024)))
    powered = math.pow(1024, log)
    size = round(_bytes / powered, 2)
    gigabyte = round(_bytes / math.pow(1024, 3), 2)
    return "{0} {1}".format(size, size_str[log]), gigabyte


class CustomTreeWidgetItem(QtWidgets.QTreeWidgetItem):
    def __init__(self, strings):
        super(CustomTreeWidgetItem, self).__init__(strings)

    def __lt__(self, other_item):
        column = self.treeWidget().sortColumn()
        if str(self.text(column)).isdigit():
            return int(str(self.text(column))) < int(str(other_item.text(column)))
        elif "GB" in self.text(column):
            regex = r"<(.+)>"
            value = re.findall(regex, self.text(column))
            other_value = re.findall(regex, other_item.text(column))
            return value < other_value
        elif QtCore.QDateTime.fromString(str(self.text(column))).isValid():
            current_time = QtCore.QDateTime.fromString(str(self.text(column)))
            other_time = QtCore.QDateTime.fromString(other_item.text(column))
            return current_time > other_time
        else:
            return str(self.text(column)).lower() > str(other_item.text(column)).lower()


class GetDeleteThread(QtCore.QThread):
    itemStatusChanged = QtCore.Signal(str, bool, int)
    itemSizeUpdated = QtCore.Signal(str, float, bool)
    itemAdded = QtCore.Signal(str, str, str, int, str)
    itemCountUpdated = QtCore.Signal(int)

    def __init__(self, parent=None):
        super(GetDeleteThread, self).__init__(parent)
        self._threads = []
        self._queue = multiprocessing.Queue()
        self._items = []

    def start(self, items):
        self._items = items
        super(GetDeleteThread, self).start()

    def run(self):
        queue = multiprocessing.Queue()

        for item in self._items:
            content, date, path = item.text(0), item.text(1), item.text(2)
            self.itemAdded.emit(content, str(), path, int(), date)
            thread = threading.Thread(target=self._calc_size, args=[path, queue])
            thread.start()
            self._threads.append(thread)

        while not queue.empty():
            path, size, num = queue.get()
            if path is None:
                break
            self.itemSizeUpdated.emit(path, size, False)
            self.itemStatusChanged.emit(path, True, num)
            self.itemCountUpdated.emit(num)

    def _calc_size(self, path, queue=None):
        size = float()
        last_size = float()
        num = int()
        file_iter = QtCore.QDirIterator(path, QtCore.QDirIterator.Subdirectories)
        while file_iter.hasNext():
            obj_file = QtCore.QFileInfo(file_iter.next())
            if obj_file.isFile():
                num += 1
                size += obj_file.size()
                if (size - last_size) > 1e+6:
                    last_size = size
                    self.itemSizeUpdated.emit(path, last_size, True)
        if queue:
            queue.put((path, size, num))
        return size


class DeleteThread(QtCore.QThread):
    itemDeleted = QtCore.Signal(str)
    fileDeleted = QtCore.Signal(str)
    itemCountUpdated = QtCore.Signal(int)
    deleteOperationFinished = QtCore.Signal()

    def __init__(self, parent=None):
        super(DeleteThread, self).__init__(parent)
        self._items = []

    def start(self, items):
        self._items = items
        super(DeleteThread, self).start()

    def run(self):
        for item in self._items:
            path = item.text(2)
            self._remove_item(str(path))
            self.itemDeleted.emit(str(path))
        self.deleteOperationFinished.emit()

    def _remove_item(self, path):
        path = path.replace("\\", "/")
        try:
            if os.path.islink(path):
                logging.error("Cannot call remove on a symbolic link")
        except OSError:
            logging.error("{0}.{1}.{2}".format(os.path.islink, path, sys.exc_info()))
            return

        i_update = int()
        last_update = int()
        for entry in scandir.scandir(path):
            if entry.is_dir():
                self._remove_item(entry.path)
            else:
                try:
                    i_update += 1
                    logging.debug("Deleting File: {0}".format(entry.path))
                    os.remove(entry.path)
                    if i_update - last_update > 50:
                        last_update = i_update
                        self.fileDeleted.emit(entry.path)
                except os.error as err:
                    logging.error(err)
            try:
                logging.debug("Deleting Dir: {0}".format(entry.path))
                os.rmdir(entry.path)
            except os.error as err:
                logging.error(err)


class QDeleteWidget(QtWidgets.QTreeWidget):
    listSizeChanged = QtCore.Signal(int)
    releaseDelete = QtCore.Signal(int, bool)
    fileDeleted = QtCore.Signal(str)
    itemCountUpdated = QtCore.Signal(int)
    itemDeleted = QtCore.Signal()
    deleteOperationFinished = QtCore.Signal(list)
    deleteListItemRemoved = QtCore.Signal(list)

    def __init__(self, parent=None):
        super(QDeleteWidget, self).__init__(parent)

        self._labels = ["Name", "Size", "Path", "File Count", "Last Modified"]
        self.path_index = self._labels.index("Path")
        self.setHeaderLabels(self._labels)
        self.header().setSectionResizeMode(QtWidgets.QHeaderView.ResizeToContents)

        self.setSortingEnabled(True)
        self.setRootIsDecorated(False)
        self.setDragDropMode(QtWidgets.QAbstractItemView.DropOnly)
        self.setSelectionMode(QtWidgets.QTreeWidget.ExtendedSelection)
        self.setContextMenuPolicy(QtCore.Qt.ActionsContextMenu)
        self.itemDoubleClicked.connect(self.open_explorer)

        self.addMenuActions()
        self._threads = []
        self._fileCount = int()
        self._removedItems = []

    def __lt__(self, other_item):
        column = self.sortColumn()
        return self.text(column).toLower() < other_item.text(column).toLower()

    def dropEvent(self, event):
        o_source = event.source()
        o_items = o_source.selectedItems()

        self.fetchMore(event.source().selectedItems())
        widget = event.source()
        for item in o_items:
            widget.remove(item)

    def addMenuActions(self):
        open_path = QtWidgets.QAction(self)
        open_path.setText("Open in explorer")
        open_path.triggered.connect(self.open_explorer)

        remove_path = QtWidgets.QAction(self)
        remove_path.setText("Remove from List")
        remove_path.triggered.connect(self.remove_item)

        self.addAction(remove_path)
        self.addAction(open_path)

    def open_explorer(self, item=None):
        items = list()
        if item:
            items.append(item)
        else:
            items = self.selectedItems()
        for item in items:
            s_path = os.path.abspath(item.text(self.path_index))
            subprocess.Popen('explorer {0}'.format(s_path))

    def remove_item(self, item=None):
        items = list()
        if item:
            items.append(item)
        else:
            items = self.selectedItems()
        for item in items:
            self.takeTopLevelItem(self.indexFromItem(item).row())

    def _add(self, content, size, path, count, last_modified):
        item = CustomTreeWidgetItem([content, size, path, str(count), last_modified])
        item.setDisabled(True)
        self.addTopLevelItem(item)

    def _status_changed(self, path, status, num):
        index = self._labels.index("Path")
        item = (self.findItems(path, QtCore.Qt.MatchExactly, index) or [None])[0]
        if item is None:
            logging.info("Could not update status: {0} {1}".format(index, path))
            return
        item.setData(20, QtCore.Qt.UserRole, status)
        item.setText(self._labels.index("File Count"), str(num))

        item.setDisabled(not status)
        self.releaseDelete.emit(num, status)

    def _size_changed(self, path, size, update=False):
        index = self.path_index
        item = (self.findItems(path, QtCore.Qt.MatchExactly, index) or [None])[0]
        if item is None:
            logging.info("Could not update size: {0} {1}".format(index, path))
            return
        column = self._labels.index("Size")
        item.setText(column, convert_size(size)[0] + "\t<{0}> GB".format(convert_size(size)[1]))
        if not update:
            self.listSizeChanged.emit(convert_size(size)[1])

    def _count_changed(self, num):
        self.itemCountUpdated.emit(num)

    def fetchMore(self, items):
        thread = GetDeleteThread()
        self._threads.append(thread)

        thread.itemAdded.connect(self._add)
        thread.itemStatusChanged.connect(self._status_changed)
        thread.itemSizeUpdated.connect(self._size_changed)

        thread.start(items)

    def file_deleted(self, path):
        self.fileDeleted.emit(path)

    def doDelete(self, selected=False):
        del self._removedItems[:]
        items = self.findItems('*', QtCore.Qt.MatchWildcard)
        if selected:
            items = self.selectedItems()
            if not items:
                QtWidgets.QMessageBox.warning(self, "Warning", "There is no item selected for deletion.")
                return

        items = [item for item in items if (item.data(20, QtCore.Qt.UserRole))]
        items_path = [str(item.text(2)) for item in items]
        c_back = QtWidgets.QMessageBox.warning(self, "Warning", "Are you sure you want to delete these files? "
                                               "This action CANNOT be undone.\n\n{0}".format(items_path),
                                               QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No
                                               )

        if not c_back == QtWidgets.QMessageBox.Yes:
            return
        for item in items:
            self._fileCount += int(item.text(3))
        thread = DeleteThread()
        thread.itemDeleted.connect(self._callback)
        thread.fileDeleted.connect(self.file_deleted)
        thread.itemCountUpdated.connect(self._count_changed)
        thread.deleteOperationFinished.connect(self._deleteDone)

        self._threads.append(thread)
        thread.start(items)

    def _deleteDone(self):
        self.deleteOperationFinished.emit(self._removedItems)

    def _callback(self, path):
        index = self._labels.index("Path")
        item = (self.findItems(path, QtCore.Qt.MatchExactly, index) or [None])[0]
        if item is None:
            logging.info("Could not delete path: {0} {1}".format(index, path))
            return

        self.takeTopLevelItem(self.indexFromItem(item).row())
        self._removedItems.append(path)

        if not self.findItems('*', QtCore.Qt.MatchWildcard):
            self.releaseDelete.emit(0, False)
        self.itemDeleted.emit()
        self.releaseDelete.emit(0, True)
        self._fileCount -= int(item.text(3))

    def getFileCount(self):
        return self._fileCount

    def getRemovedItems(self):
        return self._removedItems


class GetShowsThread(QtCore.QThread):
    showFound = QtCore.Signal(str, str, str, str)

    def __init__(self, parent=None):
        super(GetShowsThread, self).__init__(parent)

    def start(self):
        super(GetShowsThread, self).start()

    def run(self):
        for _index, show_dict in {}.items():  # sg_utilities.getShows(activeOny=False).items():
            show_name = show_dict['code']
            show_status = str(show_dict['sg_status'])
            for base_path in __BASE_PATHS__:
                path = os.path.join(base_path, show_name, 'scenes').replace('\\', '/')
                if not os.path.exists(path) or not os.listdir(path):
                    continue

                info = QtCore.QFileInfo(path)
                self.showFound.emit(show_name, show_status, info.lastModified().toString(), path)

            if show_name.startswith('vp'):
                local_path = os.path.join(LOCAL_STORAGE_DRIVE, show_name)
                if not os.path.isdir(local_path):
                    continue
                info = QtCore.QFileInfo(local_path)
                self.showFound.emit(show_name, show_status, info.lastModified().toString(), local_path)


class QShowWidget(QtWidgets.QTreeWidget):
    showPathRole = QtCore.Qt.UserRole + 21
    showPathChanged = QtCore.Signal(str)

    def __init__(self, parent=None):
        super(QShowWidget, self).__init__(parent)
        self._labels = ["Show", "Last Modified", "Path", "Status"]
        self.path_index = self._labels.index("Path")
        self.header().setSectionResizeMode(QtWidgets.QHeaderView.ResizeToContents)
        self.setHeaderLabels(self._labels)
        self.setDragEnabled(True)
        self.setSortingEnabled(True)
        self.setRootIsDecorated(False)
        self.setSelectionMode(QtWidgets.QTreeWidget.ExtendedSelection)
        self.setContextMenuPolicy(QtCore.Qt.ActionsContextMenu)
        self.itemDoubleClicked.connect(self.open_explorer)
        self.addMenuActions()
        self.itemClicked.connect(self._showClicked)

        self._removed_paths = []
        self._thread = GetShowsThread()
        self._thread.showFound.connect(self._add)

    def addMenuActions(self):
        open_path = QtWidgets.QAction(self)
        open_path.setText("Open in explorer")
        open_path.triggered.connect(self.open_explorer)
        self.addAction(open_path)

    def open_explorer(self, item=None):
        items = list()
        if item:
            items.append(item)
        else:
            items = self.selectedItems()
        for item in items:
            s_path = os.path.abspath(item.text(self.path_index))
            subprocess.Popen('explorer {0}'.format(s_path))

    def _add(self, name, status, last_modified, path):
        if path in self._removed_paths:
            return
        item = CustomTreeWidgetItem([name, last_modified, path, status])
        self.addTopLevelItem(item)

    def _showClicked(self, item):
        show_path = item.text(self.path_index)
        self.showPathChanged.emit(show_path)

    def fetchMore(self):
        if self._thread.isRunning():
            self._thread.terminate()
        self.clear()
        self._thread.start()

    def remove(self, item):
        self._removed_paths.append(item.text(self.path_index))
        index = self.indexOfTopLevelItem(item)
        self.takeTopLevelItem(index)

    def reset_data(self):
        self._removed_paths = []
        self.fetchMore()


class GetShotsThread(QtCore.QThread):
    shotFound = QtCore.Signal(str, str, str)

    def __init__(self):
        super(GetShotsThread, self).__init__()
        self._path = str()

    def run(self):
        path = str(self._path).replace("\\", "/")
        if not os.path.exists(path) or not os.listdir(path):
            return

        for entry in scandir.scandir(path):
            if not entry.is_dir():
                continue
            path = entry.path.replace("\\", "/")
            info = QtCore.QFileInfo(path)
            if not os.listdir(path):
                continue
            self.shotFound.emit(entry.name, info.lastModified().toString(), path)


class QShotWidget(QtWidgets.QTreeWidget):
    def __init__(self, parent=None):
        super(QShotWidget, self).__init__(parent)
        self._labels = ["Content", "Last Modified", "Path"]
        self.setHeaderLabels(self._labels)
        self.path_index = self._labels.index("Path")
        self.header().setSectionResizeMode(QtWidgets.QHeaderView.ResizeToContents)
        self.setRootIsDecorated(True)
        self.setSortingEnabled(True)
        self.setColumnWidth(0, 250)
        self.setDragEnabled(True)
        self.setSelectionMode(QtWidgets.QTreeWidget.ExtendedSelection)
        self.setContextMenuPolicy(QtCore.Qt.ActionsContextMenu)
        self.itemDoubleClicked.connect(self.open_explorer)

        self.addMenuActions()

        self._removed_paths = []
        self._current_path = None

        self._thread = GetShotsThread()
        self._thread.shotFound.connect(self._add)

    def addMenuActions(self):
        open_path = QtWidgets.QAction(self)
        open_path.setText("Open in explorer")
        open_path.triggered.connect(self.open_explorer)
        self.addAction(open_path)

    def open_explorer(self, item=None):
        items = list()
        if item:
            items.append(item)
        else:
            items = self.selectedItems()
        for item in items:
            s_path = os.path.abspath(item.text(self.path_index))
            subprocess.Popen('explorer {0}'.format(s_path))

    def _add(self, name, last_date, path):
        if path in self._removed_paths:
            return
        item = CustomTreeWidgetItem([name, last_date, path])
        self.addTopLevelItem(item)

    def fetchMore(self, path):
        self._current_path = path
        if self._thread.isRunning():
            self._thread.terminate()
        self.clear()
        self._thread.start(path)

    def remove(self, item):
        self._removed_paths.append(item.text(self.path_index))
        index = self.indexOfTopLevelItem(item)
        self.takeTopLevelItem(index)

    def reset_data(self):
        self._removed_paths = []
        if self._current_path:
            self.fetchMore(self._current_path)


class DriveCleanupMainWindow(QtWidgets.QDialog):
    def __init__(self):
        super(DriveCleanupMainWindow, self).__init__(None)

        self.setWindowTitle("{0} {1}".format(__TOOL_NAME__, __VERSION__))
        self.settings = QtCore.QSettings('griffin_pipeline', 'drive_cleanup_tool')
        self.setWindowFlags(QtCore.Qt.Window)
        icon_path = os.path.join(os.path.dirname(__file__), 'resources/icon.png')
        self.setWindowIcon(QtGui.QIcon(icon_path))
        self.setMinimumWidth(1200)
        self.setMinimumHeight(600)
        self.file_count = int()
        win_geometry = self.settings.value('geometry', '')
        if win_geometry:
            try:
                self.restoreGeometry(win_geometry.toByteArray())  # py2
            except Exception as e:
                logging.error(e)
                self.restoreGeometry(bytearray(win_geometry))  #py3
        shows_group_box = QtWidgets.QGroupBox("Shows List")
        self.shows = QShowWidget()
        show_layout = QtWidgets.QVBoxLayout()
        show_layout.addWidget(self.shows)
        shows_group_box.setLayout(show_layout)

        contents_group_box = QtWidgets.QGroupBox("Contents")
        self.contents = QShotWidget()

        self.shows.showPathChanged.connect(self.contents.fetchMore)
        contents_layout = QtWidgets.QVBoxLayout()
        contents_layout.addWidget(self.contents)
        contents_group_box.setLayout(contents_layout)

        delete_group_box = QtWidgets.QGroupBox("Delete List")
        self.delete = QDeleteWidget()
        self.delete.listSizeChanged.connect(self.update_progress)
        self.delete.fileDeleted.connect(self.updateRemovedCount)
        self.delete.fileDeleted.connect(self.update_message)
        self.delete.itemDeleted.connect(self.resetProgress)
        self.delete.deleteOperationFinished.connect(self.operation_callback)
        self.delete.deleteListItemRemoved.connect(self.updateData)

        self.status = QtWidgets.QStatusBar(self.delete)
        self.update_status()

        self.delete_btn = QtWidgets.QPushButton("Delete Selected")
        self.delete_all_btn = QtWidgets.QPushButton("Delete All")
        self.update_controllers()

        ctrl_layout = QtWidgets.QHBoxLayout()
        ctrl_layout.addWidget(self.status)
        ctrl_layout.addWidget(self.delete_btn)
        ctrl_layout.addWidget(self.delete_all_btn)

        delete_layout = QtWidgets.QVBoxLayout()
        delete_layout.addWidget(self.delete)
        delete_layout.addLayout(ctrl_layout)
        delete_group_box.setLayout(delete_layout)

        control_group_box = QtWidgets.QGroupBox("Information")

        self.reset_btn = QtWidgets.QPushButton("Reload Data")
        self.reset_btn.clicked.connect(self.shows.reset_data)
        self.reset_btn.clicked.connect(self.contents.reset_data)

        self.progress = QtWidgets.QProgressBar()
        self.progress.setFormat("   Delete List Size : %v GB | Total Used Size %m GB")
        self.progress.setMaximum(convert_size(psutil.disk_usage("/Users").used)[1])
        self.value = int()
        self.removedFiles = int()
        self.progress.setValue(int())

        controllers_layout = QtWidgets.QHBoxLayout()
        controllers_layout.addWidget(self.progress)
        controllers_layout.addWidget(self.reset_btn)
        control_group_box.setLayout(controllers_layout)

        self.connections()

        main_layout = QtWidgets.QGridLayout()
        main_layout.addWidget(shows_group_box, 0, 0)
        main_layout.addWidget(contents_group_box, 0, 1)
        main_layout.addWidget(delete_group_box, 1, 0, 1, 2)
        main_layout.addWidget(control_group_box, 2, 0, 1, 2)
        self.setLayout(main_layout)
        self.shows.fetchMore()

    def operation_callback(self, paths):
        message = QtWidgets.QMessageBox(self)
        message.setStyleSheet("QFrame{min-width: 250px;}")
        message.setWindowTitle("Cleanup Done")
        message.setText("{0} Item/s successfully deleted.".format(len(paths)))
        message_text = str()
        for path in paths:
            message_text += path.replace("/", "\\") + "\n"
        message.setDetailedText(message_text)
        message.setIcon(QtWidgets.QMessageBox.Information)
        message.exec_()
        self.shows.reset_data()
        self.contents.reset_data()

    def resetProgress(self):
        self.removedFiles = 0

        self.progress.setFormat("    Delete List Size: %v GB | Total User Size %m GB")
        self.progress.setMaximum(convert_size(psutil.disk_usage("/Users").used)[1])

        self.reset_btn.setEnabled(True)
        self.update_progress(reset=True)
        self.update_status()
        self.update_controllers()

    def update_message(self, path):
        if path:
            self.status.showMessage(path)

    def update_status(self):
        s_total = convert_size(psutil.disk_usage("/Users").total)[0]
        s_free = convert_size(psutil.disk_usage("/Users").free)[0]
        s_used = convert_size(psutil.disk_usage("/Users").used)[0]

        self.status.showMessage('Total Disk Size: {0:20}Used : {1:20}Free: {2:20}'.format(s_total, s_used, s_free))

    def update_progress(self, size=int(), reset=False):
        if size:
            self.value += size
        if reset:
            self.value = int()
        self.progress.setValue(self.value)

    def connections(self):
        self.reset_btn.clicked.connect(lambda : self.reset_btn())
        self.delete_all_btn.clicked.connect(lambda : self.delete.doDelete())
        self.delete_btn.clicked.connect(lambda : self.delete.doDelete(selected=True))
        self.delete.releaseDelete.connect(self.update_controllers)

    def update_controllers(self):
        items = self.delete.findItems('*', QtCore.Qt.MatchWildcard)
        self.delete_all_btn.setEnabled(bool(items))
        self.delete_btn.setEnabled(bool(items))

    def updateRemovedCount(self, path):
        self.removedFiles += 60
        self.delete_progress()
        self.progress.setValue(self.removedFiles)

    def delete_progress(self):
        self.progress.setFormat("    Removed Filed: %v | Number of Files %m")
        self.progress.setMaximum(self.delete.getFileCount())
        self.reset_btn.setEnabled(False)

    def reset(self):
        self.delete_btn.setEnabled(False)
        self.delete_all_btn.setEnabled(False)
        self.delete.clear()
        self.update_progress(reset=True)
        self.update_progress()

    def updateData(self, items):
        update_paths = list()
        for item in items:
            path = item.text(2)
            if path in self.shows._removed_paths:
                self.shows._removed_paths.remove(path)
                self.shows.reset_data()
            elif path in self.contents._removed_paths:
                self.contents._removed_paths.remove(path)
                update_paths.append(path)

        if len(update_paths):
            paths = [os.path.dirname(str(path)) for path in update_paths]
            paths = set(paths)
            for path in paths:
                self.contents.fetchMore(path)

    def closeEvent(self, event):
        win_geometry = self.saveGeometry()
        self.settings.setValue('geometry', win_geometry)
        super(DriveCleanupMainWindow, self).closeEvent(event)


def launch():
    dialog = DriveCleanupMainWindow()
    dialog.show()


if __name__ == '__main__':
    app = QtWidgets.QApplication(sys.argv)
    dialog = DriveCleanupMainWindow()
    dialog.show()
    app.exec_()
