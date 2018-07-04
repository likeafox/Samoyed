import kivy
kivy.require('1.10.0')
from kivy.app import App
from kivy.uix.widget import Widget
from kivy.uix.label import Label
from kivy.uix.gridlayout import GridLayout
from kivy.uix.textinput import TextInput
from kivy.uix.switch import Switch
from kivy.uix.boxlayout import BoxLayout
from kivy.lang import Builder
from kivy.uix.widget import Widget
from kivy.graphics import Color, Rectangle
from kivy.properties import ListProperty



def find(_id=None):
    root = App.get_running_app().root
    return root if _id is None else root.ids[_id]

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
        self.update('stopped')

    def update(self, state):
        valid_states = dict(
            stopped = ('Not connected', [0.4, 0.4, 0.4, 1]),
            startup = ('Starting up', [1, 1, 0.7, 1]),
            running = ('Running', [0, 0.65, 0, 1]),
            stop = ('Shutting down', [0.6, 0.1, 0.1, 1]))
        caption, color = valid_states[state]
        self.text = "Service: " + caption
        self.color = [0,0,0,1] if sum(color) > 3 else [1,1,1,1]
        if self.parent is not None:
            self.parent.color = color

class SettingsPage(BoxLayout):
    pass

Builder.load_string('''
<Row>:
    size_hint_max_y: 50
    canvas.before:
        Color:
            rgba: self.color
        Rectangle:
            pos: self.pos
            size: self.size

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
        Label:
            text: 'Samoyed v0.1'
            color: 0.1,0.1,0.2,1
        OnSwitch

    Row:
        Status:
            id: status

    Row:
        Label:
            text: 'username'
        ConfigText
            id: username

    Row:
        Button:
            text: 'generate access key with a password'

    Row:
        orientation: 'vertical'
        Label:
            text: 'server url'
        ConfigText:
            id: server_url

    Row:
        orientation: 'vertical'
        Label:
            text: 'access key'
        ConfigText:
            id: access_key

    Widget:

<ConfigText@TextInput>:
    multiline: False
    size_hint_min_y: 30
''')

class SamoyedApp(App):
    def build(self):
        r = SettingsPage()
        return r


if __name__ == '__main__':
    SamoyedApp().run()

