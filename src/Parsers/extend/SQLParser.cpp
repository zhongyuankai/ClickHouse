#include <Parsers/QueryParseTools.h>

#include <iostream>

int main(int, char **)
{
    while (true)
    {
        int length;
        std::cin >> length;
        if (length <= 0)
        {
            std::cout << "length must greater then 0" << '\n';
            break;
        }

        if (char s = std::cin.get(); s != '#')
        {
            std::cout << "expect delimiter '#' but found: " << s << '\n';
            break;
        }

        String input;
        input.resize(length);
        std::cin.read(input.data(), length);

        String result = DB::parseAnyDatabaseAndTable(input);
        std::cout << result.length() << '#' << result;
        std::cout.flush();
    }
    return 0;
}

