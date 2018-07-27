import kivy
kivy.require('1.10.0')
from kivy.app import App
from kivy.uix.widget import Widget
from kivy.uix.label import Label
from kivy.uix.gridlayout import GridLayout
from kivy.uix.textinput import TextInput
from kivy.uix.switch import Switch
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.stacklayout import StackLayout
from kivy.lang import Builder
from kivy.uix.widget import Widget
from kivy.graphics import Color, Rectangle
from kivy.properties import ListProperty
from kivy.uix.scrollview import ScrollView
from kivy.core.window import Window
import jnius
import json

#local import
from buildnum import BUILD_NUMBER

VERSION = (0,1,0)
DEBUG = True


VERSION_STR = '.'.join(map(str,VERSION))
FULL_TITLE = "Samoyed {} for Android, {} build {}".format(
                VERSION_STR,("DEBUG" if DEBUG else "RELEASE"),BUILD_NUMBER)

settings_fields = set(
    "user_key server_url access_key nickname"
    .split())

def find(_id=None):
    root = App.get_running_app().root
    for w in root.walk():
        if isinstance(w, SettingsPage):
            return w if _id is None else w.ids[_id]
    raise Exception("Can't find settings page")

def post_msg(msg, debug=False):
    if debug and not DEBUG:
        return
    msg = str(msg)
    print(msg)
    find('output').text += msg + '\n'

class Row(BoxLayout):
    color = ListProperty([0.5,0.5,0.7,1])

class OnSwitch(Switch):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.bind(active=__class__.was_switched)

    def was_switched(self, value):
        find('status').update('stopped' if not value else 'startup')

class Status(Label):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def update(self, state):
        valid_states = dict(
            stopped = ('Not connected', [0.4, 0.4, 0.4, 1]),
            startup = ('Starting up', [1, 1, 0.7, 1]),
            running = ('Running', [0, 0.65, 0, 1]),
            stop = ('Shutting down', [0.6, 0.1, 0.1, 1]))
        caption, color = valid_states[state]
        self.state = state
        self.text = "Service: " + caption
        self.color = [0,0,0,1] if sum(color) > 3 else [1,1,1,1]
        if self.parent is not None:
            self.parent.color = color

class InvalidSettings(Exception): pass

class SettingsPage(BoxLayout):
    def _get_pref(self):
        activity = App.get_running_app().activity
        return activity.getSharedPreferences(VERSION_STR,0)

    def load(self):
        settings_json = self._get_pref().getString('settings','{}')
        self.settings = json.loads(settings_json)
        for fieldname,v in self.settings.items():
            if fieldname in settings_fields:
                find(fieldname).text = v

    def save(self):
        for fieldname in settings_fields:
            v = find(fieldname).text
            if v != "" or fieldname in self.settings:
                self.settings[fieldname] = v
        settings_json = json.dumps(self.settings)
        #save
        editor = self._get_pref().edit()
        editor.putString('settings',settings_json)
        editor.commit()

Builder.load_string('''
<Row>:
    size_hint: 1, None
    height: self.minimum_height
    #height: sp(15) + mm(10)
    padding: mm(4)
    canvas.before:
        Color:
            rgba: self.color
        Rectangle:
            pos: self.pos
            size: self.size

<ConfigText@TextInput>:
    size_hint: 1, None
    pos_hint: {'x':0,'center_y':0.5}
    height: mm(8)
    font_size: mm(4)
    multiline: False

<L@Label>:
    pos_hint: {'center_y':0.5}
    size_hint: None, None
    height: sp(15) + mm(2)

<SettingsPage>:
    orientation: 'vertical'
    spacing: 2
    canvas.before:
        Color:
            rgba: 0.3, 0.3, 0.4, 1
        Rectangle:
            pos: self.pos
            size: self.size

    Row:
        color: 0.9, 0.9, 0.95, 1
        L:
            text: 'Samoyed v0.1'
            color: 0.1,0.1,0.2,1
            size_hint_x: 1
        OnSwitch:
            size_hint: None, 1

    Row:
        height: sp(15) + mm(10)
        padding: 0
        Status:
            id: status

    Row:
        orientation: 'vertical'
        L:
            text: 'server url'
        ConfigText:
            id: server_url

    Row:
        orientation: 'vertical'
        L:
            text: 'access key'
        ConfigText:
            id: access_key

    Row:
        L:
            text: 'username'
            size_hint_x: 0.5
        ConfigText
            size_hint_x: 0.5
            id: nickname

    Row:
        L:
            text: 'password'
        ConfigText:
            id: password
        Button:
            size_hint: None, None
            pos_hint: {'center_y':0.5}
            size: self.texture_size[0] + mm(4), sp(15) + mm(4)
            text: 'gen user key'

    Row:
        orientation: 'vertical'
        L:
            text: 'user key'
        ConfigText:
            id: user_key

    Label:
        size_hint_y: None
        text_size: self.width, None
        height: self.texture_size[1]
        id: output

    Widget

<ScrollView>:
    SettingsPage:
        #some properties required to scroll properly:
        size_hint: 1, None
        height: self.minimum_height
''')

class SamoyedApp(App):
    def build(self):
        self.title = FULL_TITLE
        r = ScrollView()
        return r

    def on_pause(self):
        find().save()
        return True

    def on_stop(self):
        find().save()
        return True

    def on_start(self):
        post_msg(FULL_TITLE)

        PythonActivity = jnius.autoclass('org.kivy.android.PythonActivity')
        self.activity = PythonActivity.mActivity

        #init controls
        find('status').update('stopped')
        find().load()

        #path_obj = activity.getFilesDir()
        #files_dir = path_obj.getAbsolutePath()
        #find('access_key').text = files_dir
        #except Exception as e:
        #    #import traceback
        #    #r = traceback.format_exc()
        #    raise
            

if __name__ == '__main__':
    SamoyedApp().run()

