MAIN_MENU_PROMPT = """
--- Main Menu ---

1) Menu 1
2) Menu 2
3) Menu 3
q) Exit Main Menu

Enter your choise:"""


def menu_1():
    print('Menu 1')


def menu_2():
    print('Menu 2')


def menu_3():
    print('Menu 3')


MAIN_MENU_OPTIONS = {
    '1': menu_1,
    '2': menu_2,
    '3': menu_3
}


def main_menu():
    while (selection := input(MAIN_MENU_PROMPT)) != 'q':
        try:
            MAIN_MENU_OPTIONS[selection]()
        except KeyError:
            print('Invalid Input Selection. Please trye again!')


if __name__ == '__main__':
    main_menu()