def RepresentsInt(s):
    try: 
        float(s)
        return True
    except ValueError:
        return False

print(RepresentsInt("123"))
print(RepresentsInt("-123"))
print(RepresentsInt("123.01"))
print(RepresentsInt("A1"))