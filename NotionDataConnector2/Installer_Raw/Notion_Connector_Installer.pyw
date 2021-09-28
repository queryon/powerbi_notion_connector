import os
import shutil

import ctypes  # An included library with Python install.

DocumentsPath = os.path.expanduser('~/Documents') #Get Documents Folder

#Does Power Bi Desktop Folder exist in Documents. If not then create it
CheckFirstFolder = os.path.exists(DocumentsPath + "\Power Bi Desktop") 
if(CheckFirstFolder == False):
    os.makedirs(DocumentsPath + "\Power Bi Desktop")
    
    
#Does Power Bi Desktop\Custom Connectors Folder exist in Documents. If not then create it
CheckFirstFolder2 = os.path.exists(DocumentsPath + "\Power Bi Desktop\Custom Connectors") 
if(CheckFirstFolder2 == False):
    DocumentsPath2 = DocumentsPath + "\Power Bi Desktop"
    os.makedirs(DocumentsPath2 + "\Custom Connectors")


#Insert the Notion.mez conenctor and overwrite old version if necessary
FullPathForConnector = DocumentsPath + "\Power Bi Desktop\Custom Connectors"
CurrentPath          = os.path.dirname(os.path.realpath(__file__))
CurrentPathWithMez   = CurrentPath + "\\Notion.mez"

newPath = shutil.copy(CurrentPathWithMez, FullPathForConnector)

ctypes.windll.user32.MessageBoxW(0, "The Notion to Power Bi Connector by Queryon was sucessfully installed! Please Visit Queryon.com/notion for instructions on how to use this connector.", "Notion to Power Bi Connector", 1)

