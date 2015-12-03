import prepare


class TestPrepare(object):
    item = prepare.Prepare().read('./testdata/test1.csv')
    def test_read(self):
        result = self.item.toMatrix()
        assert len(result) == 6

    def test_addColumn(self):
        result = self.item.addColumn('append1', [1,2,3,4,5,6]).toMatrix()
        assert len(result[0]) == 7

    def test_applyColumnEvent(self):
        result = self.item.applyColumnEvent('nice', lambda x: 1 if x == 'A' else 2).toDF()
        expected = {'nice': {0: 1, 1: 2, 2: 2, 3: 2, 4: 2, 5: 2}}
        assert result[['nice']].to_dict() == expected
